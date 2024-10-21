/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.facebook.presto.cost;

import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.StatsPropagationBehavior;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.type.IntervalYearMonthType;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.spi.function.StatsPropagationBehavior.Constants.NON_NULL_ROW_COUNT_CONST;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.Constants.ROW_COUNT_CONST;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.SUBSTR_ROW_SIZE;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.SUM_ARGUMENTS;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.SUM_ARGUMENTS_UPPER_BOUNDED_TO_ROW_COUNT;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.USE_SOURCE_STATS;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.facebook.presto.util.MoreMath.minExcludingNaNs;
import static com.facebook.presto.util.MoreMath.nearlyEqual;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public final class ScalarStatsAnnotationProcessor
{
    private ScalarStatsAnnotationProcessor()
    {
    }

    public static VariableStatsEstimate computeConcatStatistics(CallExpression call, List<VariableStatsEstimate> sourceStats, double outputRowCount)
    {  // Concat function is specially handled since it is a generated function for all arity.
        double nullFraction = NaN;
        double ndv = NaN;
        double avgRowSize = 0.0;
        for (VariableStatsEstimate stat : sourceStats) {
            if (isFinite(stat.getNullsFraction())) {
                nullFraction = firstFiniteValue(nullFraction, 0.0);
                nullFraction = max(nullFraction, stat.getNullsFraction());
            }
            if (isFinite(stat.getDistinctValuesCount())) {
                ndv = firstFiniteValue(ndv, 0.0);
                ndv = max(ndv, stat.getDistinctValuesCount());
            }
            if (isFinite(stat.getAverageRowSize())) {
                avgRowSize += stat.getAverageRowSize();
            }
        }
        if (avgRowSize == 0.0) {
            avgRowSize = NaN;
        }
        return VariableStatsEstimate.builder()
                .setNullsFraction(nullFraction)
                .setDistinctValuesCount(minExcludingNaNs(ndv, outputRowCount))
                .setAverageRowSize(minExcludingNaNs(returnNaNIfTypeWidthUnknown(getReturnTypeWidth(call, SUM_ARGUMENTS)), avgRowSize))
                .build();
    }

    public static VariableStatsEstimate computeHashCodeOperatorStatistics(CallExpression call, List<VariableStatsEstimate> sourceStats, double outputRowCount)
    {
        requireNonNull(call, "call is null");
        checkArgument(sourceStats.size() == 1,
                "exactly one argument expected for hash code operator scalar function");
        VariableStatsEstimate argStats = sourceStats.get(0);
        VariableStatsEstimate.Builder result =
                VariableStatsEstimate.builder()
                        .setAverageRowSize(returnNaNIfTypeWidthUnknown(getReturnTypeWidth(call, UNKNOWN)))
                        .setNullsFraction(argStats.getNullsFraction())
                        .setDistinctValuesCount(minExcludingNaNs(argStats.getDistinctValuesCount(), outputRowCount));
        return result.build();
    }

    public static VariableStatsEstimate computeComparisonOperatorStatistics(CallExpression call, List<VariableStatsEstimate> sourceStats)
    {
        requireNonNull(call, "call is null");
        if (sourceStats.size() != 2) {
            return VariableStatsEstimate.unknown();
        }
        VariableStatsEstimate left = sourceStats.get(0);
        VariableStatsEstimate right = sourceStats.get(1);
        VariableStatsEstimate.Builder result =
                VariableStatsEstimate.builder()
                        .setAverageRowSize(returnNaNIfTypeWidthUnknown(getReturnTypeWidth(call, UNKNOWN)))
                        .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                        .setDistinctValuesCount(1.0);
        return result.build();
    }

    public static VariableStatsEstimate computeYearFunctionStatistics(CallExpression call, List<VariableStatsEstimate> sourceStats)
    {
        ISOChronology utcChronology = ISOChronology.getInstanceUTC();
        DateTimeField YEAR = utcChronology.year();

        if (sourceStats.size() != 1) {
            return VariableStatsEstimate.unknown();
        }
        VariableStatsEstimate date = sourceStats.get(0);
        VariableStatsEstimate.Builder result = VariableStatsEstimate.builder();
        if (isFinite(date.getLowValue()) && isFinite(date.getHighValue()) && call.getArguments().get(0).getType() instanceof DateType) {
            int minYear = YEAR.get(DAYS.toMillis(Double.valueOf(date.getLowValue()).longValue()));
            int maxYear = YEAR.get(DAYS.toMillis(Double.valueOf(date.getHighValue()).longValue()));
            int ndv = maxYear - minYear;
            result.setDistinctValuesCount(minExcludingNaNs(ndv, date.getDistinctValuesCount()));
            result.setLowValue(minYear);
            result.setHighValue(maxYear);
        }
        else {
            System.out.println("Stats unknown for " + call);
        }
        result.setAverageRowSize(returnNaNIfTypeWidthUnknown(getReturnTypeWidth(call, UNKNOWN)))
                .setNullsFraction(date.getNullsFraction());
        return result.build();
    }

    public static VariableStatsEstimate computeStatsFromAnnotations(
            CallExpression callExpression,
            List<VariableStatsEstimate> sourceStats,
            ScalarStatsHeader scalarStatsHeader,
            double outputRowCount)
    {
        double nullFraction = scalarStatsHeader.getNullFraction();
        double distinctValuesCount = NaN;
        double averageRowSize = NaN;
        double maxValue = scalarStatsHeader.getMax();
        double minValue = scalarStatsHeader.getMin();
        for (Map.Entry<Integer, ScalarPropagateSourceStats> paramIndexToStatsMap : scalarStatsHeader.getArgumentStats().entrySet()) {
            ScalarPropagateSourceStats scalarPropagateSourceStats = paramIndexToStatsMap.getValue();
            boolean propagateAllStats = scalarPropagateSourceStats.propagateAllStats();
            nullFraction = min(firstFiniteValue(nullFraction, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getNullsFraction).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.nullFraction()))), 1.0);
            distinctValuesCount = firstFiniteValue(distinctValuesCount, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getDistinctValuesCount).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.distinctValuesCount())));
            StatsPropagationBehavior averageRowSizeStatsBehaviour = applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.avgRowSize());
            averageRowSize = minExcludingNaNs(firstFiniteValue(averageRowSize, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getAverageRowSize).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    averageRowSizeStatsBehaviour)), returnNaNIfTypeWidthUnknown(getReturnTypeWidth(callExpression, averageRowSizeStatsBehaviour)));
            maxValue = firstFiniteValue(maxValue, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getHighValue).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.maxValue())));
            minValue = firstFiniteValue(minValue, processSingleArgumentStatistic(outputRowCount, nullFraction, callExpression,
                    sourceStats.stream().map(VariableStatsEstimate::getLowValue).collect(toImmutableList()),
                    paramIndexToStatsMap.getKey(),
                    applyPropagateAllStats(propagateAllStats, scalarPropagateSourceStats.minValue())));
        }
        if (isNaN(maxValue) || isNaN(minValue)) {
            minValue = NaN;
            maxValue = NaN;
        }
        return VariableStatsEstimate.builder()
                .setLowValue(minValue)
                .setHighValue(maxValue)
                .setNullsFraction(nullFraction)
                .setAverageRowSize(firstFiniteValue(scalarStatsHeader.getAvgRowSize(), averageRowSize, returnNaNIfTypeWidthUnknown(getReturnTypeWidth(callExpression, UNKNOWN))))
                .setDistinctValuesCount(processDistinctValuesCount(outputRowCount, nullFraction, scalarStatsHeader.getDistinctValuesCount(), distinctValuesCount)).build();
    }

    private static double processDistinctValuesCount(double outputRowCount, double nullFraction, double distinctValuesCountFromConstant, double distinctValuesCount)
    {
        if (isFinite(distinctValuesCountFromConstant)) {
            if (nearlyEqual(distinctValuesCountFromConstant, NON_NULL_ROW_COUNT_CONST, 0.1)) {
                distinctValuesCountFromConstant = outputRowCount * (1 - firstFiniteValue(nullFraction, 0.0));
            }
            else if (nearlyEqual(distinctValuesCount, ROW_COUNT_CONST, 0.1)) {
                distinctValuesCountFromConstant = outputRowCount;
            }
        }
        double distinctValuesCountFinal = firstFiniteValue(distinctValuesCountFromConstant, distinctValuesCount);
        if (distinctValuesCountFinal > outputRowCount) {
            distinctValuesCountFinal = NaN;
        }
        return distinctValuesCountFinal;
    }

    private static double processSingleArgumentStatistic(
            double outputRowCount,
            double nullFraction,
            CallExpression callExpression,
            List<Double> sourceStats,
            int sourceStatsArgumentIndex,
            StatsPropagationBehavior operation)
    {
        // sourceStatsArgumentIndex is index of the argument on which
        // ScalarPropagateSourceStats annotation was applied.
        double statValue = NaN;
        if (operation.isMultiArgumentStat()) {
            for (int i = 0; i < sourceStats.size(); i++) {
                if (i == 0 && operation.isSourceStatsDependentStats() && isFinite(sourceStats.get(i))) {
                    statValue = sourceStats.get(i);
                }
                else {
                    switch (operation) {
                        case MAX_TYPE_WIDTH_VARCHAR:
                            statValue = returnNaNIfTypeWidthUnknown(getTypeWidth(callExpression.getArguments().get(i).getType()));
                            break;
                        case USE_MIN_ARGUMENT:
                            statValue = min(statValue, sourceStats.get(i));
                            break;
                        case USE_MAX_ARGUMENT:
                            statValue = max(statValue, sourceStats.get(i));
                            break;
                        case SUM_ARGUMENTS:
                            statValue = statValue + sourceStats.get(i);
                            break;
                        case SUM_ARGUMENTS_UPPER_BOUNDED_TO_ROW_COUNT:
                            statValue = min(statValue + sourceStats.get(i), outputRowCount);
                            break;
                    }
                }
            }
        }
        else {
            switch (operation) {
                case USE_SOURCE_STATS:
                    statValue = sourceStats.get(sourceStatsArgumentIndex);
                    break;
                case CONDITIONALLY_USE_SOURCE_STATS:
                    if (sourceStats.get(sourceStatsArgumentIndex) < (outputRowCount / 100)) {
                        statValue = sourceStats.get(sourceStatsArgumentIndex);
                    }
                    break;
                case ROW_COUNT:
                    statValue = outputRowCount;
                    break;
                case NON_NULL_ROW_COUNT:
                    statValue = outputRowCount * (1 - firstFiniteValue(nullFraction, 0.0));
                    break;
                case USE_TYPE_WIDTH_VARCHAR:
                    statValue = returnNaNIfTypeWidthUnknown(getTypeWidth(callExpression.getArguments().get(sourceStatsArgumentIndex).getType()));
                    break;
                case LOG10_SOURCE_STATS:
                    statValue = Math.log10(sourceStats.get(sourceStatsArgumentIndex));
                    break;
                case LOG2_SOURCE_STATS:
                    statValue = Math.log(sourceStats.get(sourceStatsArgumentIndex)) / Math.log(2);
                    break;
                case LOG_NATURAL_SOURCE_STATS:
                    statValue = Math.log(sourceStats.get(sourceStatsArgumentIndex));
            }
        }
        return statValue;
    }

    private static int getTypeWidth(Type argumentType)
    {
        if (argumentType instanceof VarcharType) {
            if (!((VarcharType) argumentType).isUnbounded()) {
                return ((VarcharType) argumentType).getLengthSafe();
            }
        }
        if (argumentType instanceof CharType) {
            return ((CharType) argumentType).getLength();
        }
        return -VarcharType.MAX_LENGTH;
    }

    private static double returnNaNIfTypeWidthUnknown(long typeWidthValue)
    {
        if (typeWidthValue <= 0) {
            return NaN;
        }
        return typeWidthValue;
    }

    private static long getReturnTypeWidth(CallExpression callExpression, StatsPropagationBehavior operation)
    {
        if (callExpression.getType() instanceof FixedWidthType) {
            return ((FixedWidthType) callExpression.getType()).getFixedSize();
        }
        if (callExpression.getType() instanceof CharType) {
            return ((CharType) callExpression.getType()).getLength();
        }
        if (callExpression.getType() instanceof VarcharType) {
            VarcharType returnType = (VarcharType) callExpression.getType();
            if (!returnType.isUnbounded()) {
                return returnType.getLengthSafe();
            }
            if (operation == SUM_ARGUMENTS || operation == SUM_ARGUMENTS_UPPER_BOUNDED_TO_ROW_COUNT) {
                // since return type is an unbounded varchar and operation is SUM_ARGUMENTS,
                // calculating the type width by doing a SUM of each argument's varchar type bounds - if available.
                long sum = 0;
                for (RowExpression r : callExpression.getArguments()) {
                    long typeWidth;
                    if (r instanceof CallExpression) { // argument is another function call
                        typeWidth = getReturnTypeWidth((CallExpression) r, UNKNOWN);
                    }
                    else {
                        typeWidth = getTypeWidth(r.getType());
                    }
                    if (typeWidth < 0) {
                        return -VarcharType.MAX_LENGTH;
                    }
                    sum += typeWidth;
                }
                return sum;
            }
            else if (operation == SUBSTR_ROW_SIZE) {
                // if substring length argument is a constant expression.
                int argSize = callExpression.getArguments().size();
                if (argSize == 2 || argSize == 3) {
                    long argumentTypeWidth = getTypeWidth(callExpression.getArguments().get(0).getType());
                    RowExpression lengthExpression = callExpression.getArguments().get(1);
                    if (argSize == 3) {
                        lengthExpression = callExpression.getArguments().get(2);
                    }
                    if (lengthExpression instanceof ConstantExpression) {
                        ConstantExpression expression = (ConstantExpression) lengthExpression;
                        if (expression.getValue() instanceof Long) {
                            long estimatedReturnTypeWidth = argumentTypeWidth - Math.abs((Long) expression.getValue());
                            if (estimatedReturnTypeWidth > 0) {
                                return estimatedReturnTypeWidth;
                            }
                        }
                    }
                }
            }
        }
        return -VarcharType.MAX_LENGTH;
    }

    // Return first 'finite' value from values, else return values[0]
    private static double firstFiniteValue(double... values)
    {
        checkArgument(values.length > 1);
        for (double v : values) {
            if (isFinite(v)) {
                return v;
            }
        }
        return values[0];
    }

    private static StatsPropagationBehavior applyPropagateAllStats(
            boolean propagateAllStats, StatsPropagationBehavior operation)
    {
        if (operation == UNKNOWN && propagateAllStats) {
            return USE_SOURCE_STATS;
        }
        return operation;
    }
}
