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

import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.StatsPropagationBehavior;
import com.facebook.presto.spi.relation.CallExpression;

import java.util.List;
import java.util.Map;

import static com.facebook.presto.cost.ScalarStatsCalculatorUtils.firstFiniteValue;
import static com.facebook.presto.cost.ScalarStatsCalculatorUtils.getReturnTypeWidth;
import static com.facebook.presto.cost.ScalarStatsCalculatorUtils.getTypeWidth;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.Constants.NON_NULL_ROW_COUNT_CONST;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.Constants.ROW_COUNT_CONST;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.USE_SOURCE_STATS;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.facebook.presto.util.MoreMath.minExcludingNaNs;
import static com.facebook.presto.util.MoreMath.nearlyEqual;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;

public final class ScalarStatsAnnotationProcessor
{
    private ScalarStatsAnnotationProcessor()
    {
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
                    averageRowSizeStatsBehaviour)), getReturnTypeWidth(callExpression, averageRowSizeStatsBehaviour));
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
                .setAverageRowSize(firstFiniteValue(scalarStatsHeader.getAvgRowSize(), averageRowSize, getReturnTypeWidth(callExpression, UNKNOWN)))
                .setDistinctValuesCount(processDistinctValuesCount(outputRowCount, nullFraction, scalarStatsHeader.getDistinctValuesCount(), distinctValuesCount)).build();
    }

    private static double processDistinctValuesCount(double outputRowCount, double nullFraction, double distinctValuesCountFromConstant, double distinctValuesCount)
    {
        if (isFinite(distinctValuesCountFromConstant)) {
            if (nearlyEqual(distinctValuesCountFromConstant, NON_NULL_ROW_COUNT_CONST, 0.1)) {
                distinctValuesCountFromConstant = outputRowCount * (1 - firstFiniteValue(nullFraction, 0.0));
            }
            else if (nearlyEqual(distinctValuesCountFromConstant, ROW_COUNT_CONST, 0.1)) {
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
                            statValue = getTypeWidth(callExpression.getArguments().get(i).getType());
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
                    }
                }
            }
        }
        else {
            switch (operation) {
                case USE_SOURCE_STATS:
                    statValue = sourceStats.get(sourceStatsArgumentIndex);
                    break;
                case ROW_COUNT:
                    statValue = outputRowCount;
                    break;
                case NON_NULL_ROW_COUNT:
                    statValue = outputRowCount * (1 - firstFiniteValue(nullFraction, 0.0));
                    break;
                case USE_TYPE_WIDTH_VARCHAR:
                    statValue = getTypeWidth(callExpression.getArguments().get(sourceStatsArgumentIndex).getType());
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

    private static StatsPropagationBehavior applyPropagateAllStats(
            boolean propagateAllStats, StatsPropagationBehavior operation)
    {
        if (operation == UNKNOWN && propagateAllStats) {
            return USE_SOURCE_STATS;
        }
        return operation;
    }
}
