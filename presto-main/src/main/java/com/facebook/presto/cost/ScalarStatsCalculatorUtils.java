package com.facebook.presto.cost;

import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.DateType;
import com.facebook.presto.common.type.FixedWidthType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.StatsPropagationBehavior;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.type.TypeUtils;
import org.joda.time.DateTimeField;
import org.joda.time.chrono.ISOChronology;

import java.util.List;

import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.MODULUS;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.SUM_ARGUMENTS;
import static com.facebook.presto.spi.function.StatsPropagationBehavior.UNKNOWN;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.facebook.presto.util.MoreMath.minExcludingNaNs;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.DAYS;

public final class ScalarStatsCalculatorUtils
{
    private ScalarStatsCalculatorUtils()
    {
    }

    public static double getTypeWidth(Type argumentType)
    {
        if (argumentType instanceof FixedWidthType) {
            return ((FixedWidthType) argumentType).getFixedSize();
        }
        if (argumentType instanceof VarcharType) {
            if (!((VarcharType) argumentType).isUnbounded()) {
                return ((VarcharType) argumentType).getLengthSafe();
            }
        }
        if (argumentType instanceof CharType) {
            return ((CharType) argumentType).getLength();
        }
        return NaN;
    }

    public static double getReturnTypeWidth(CallExpression callExpression, StatsPropagationBehavior operation)
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
            if (operation == SUM_ARGUMENTS) {
                // since return type is an unbounded varchar and operation is SUM_ARGUMENTS,
                // calculating the type width by doing a SUM of each argument's varchar type bounds - if available.
                double sum = 0;
                for (RowExpression r : callExpression.getArguments()) {
                    double typeWidth;
                    if (r instanceof CallExpression) { // argument is another function call
                        typeWidth = getReturnTypeWidth((CallExpression) r, UNKNOWN);
                    }
                    else {
                        typeWidth = getTypeWidth(r.getType());
                    }
                    if (typeWidth < 0) {
                        return NaN;
                    }
                    sum += typeWidth;
                }
                return sum;
            }
        }
        return NaN;
    }

    // Return first 'finite' value from values, else return values[0]
    public static double firstFiniteValue(double... values)
    {
        checkArgument(values.length > 1);
        for (double v : values) {
            if (isFinite(v)) {
                return v;
            }
        }
        return values[0];
    }

    public static VariableStatsEstimate computeCastStatistics(CallExpression callExpression, Metadata metadata, List<VariableStatsEstimate> sourceStats)
    {
        requireNonNull(callExpression, "call is null");
        checkArgument(!sourceStats.isEmpty());
        // todo - make this general postprocessing rule.
        double distinctValuesCount = sourceStats.get(0).getDistinctValuesCount();
        double lowValue = sourceStats.get(0).getLowValue();
        double highValue = sourceStats.get(0).getHighValue();

        if (TypeUtils.isIntegralType(callExpression.getType().getTypeSignature(), metadata.getFunctionAndTypeManager())) {
            // todo handle low/high value changes if range gets narrower due to cast (e.g. BIGINT -> SMALLINT)
            if (isFinite(lowValue)) {
                lowValue = Math.round(lowValue);
            }
            if (isFinite(highValue)) {
                highValue = Math.round(highValue);
            }
            if (isFinite(lowValue) && isFinite(highValue)) {
                double integersInRange = highValue - lowValue + 1;
                if (!isNaN(distinctValuesCount) && distinctValuesCount > integersInRange) {
                    distinctValuesCount = integersInRange;
                }
            }
        }

        return VariableStatsEstimate.builder()
                .setNullsFraction(sourceStats.get(0).getNullsFraction())
                .setLowValue(lowValue)
                .setHighValue(highValue)
                .setDistinctValuesCount(distinctValuesCount)
                .build();
    }

    public static VariableStatsEstimate computeArithmeticBinaryStatistics(FunctionMetadata functionMetadata, List<VariableStatsEstimate> sourceStats, double outputRowCount)
    {
        checkArgument(sourceStats.size() > 1);
        VariableStatsEstimate left = sourceStats.get(0);
        VariableStatsEstimate right = sourceStats.get(1);

        VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), outputRowCount));
        checkState(functionMetadata.getOperatorType().isPresent());
        OperatorType operatorType = functionMetadata.getOperatorType().get();
        double leftLow = left.getLowValue();
        double leftHigh = left.getHighValue();
        double rightLow = right.getLowValue();
        double rightHigh = right.getHighValue();
        if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
            result.setLowValue(NaN).setHighValue(NaN);
        }
        else if (operatorType.equals(DIVIDE) && rightLow < 0 && rightHigh > 0) {
            result.setLowValue(Double.NEGATIVE_INFINITY)
                    .setHighValue(Double.POSITIVE_INFINITY);
        }
        else if (operatorType.equals(MODULUS)) {
            double maxDivisor = max(abs(rightLow), abs(rightHigh));
            if (leftHigh <= 0) {
                result.setLowValue(max(-maxDivisor, leftLow))
                        .setHighValue(0);
            }
            else if (leftLow >= 0) {
                result.setLowValue(0)
                        .setHighValue(min(maxDivisor, leftHigh));
            }
            else {
                result.setLowValue(max(-maxDivisor, leftLow))
                        .setHighValue(min(maxDivisor, leftHigh));
            }
        }
        else {
            double v1 = operate(operatorType, leftLow, rightLow);
            double v2 = operate(operatorType, leftLow, rightHigh);
            double v3 = operate(operatorType, leftHigh, rightLow);
            double v4 = operate(operatorType, leftHigh, rightHigh);
            double lowValue = min(v1, v2, v3, v4);
            double highValue = max(v1, v2, v3, v4);

            result.setLowValue(lowValue)
                    .setHighValue(highValue);
        }

        return result.build();
    }

    private static double operate(OperatorType operator, double left, double right)
    {
        switch (operator) {
            case ADD:
                return left + right;
            case SUBTRACT:
                return left - right;
            case MULTIPLY:
                return left * right;
            case DIVIDE:
                return left / right;
            case MODULUS:
                return left % right;
            default:
                throw new IllegalStateException("Unsupported ArithmeticBinaryExpression.Operator: " + operator);
        }
    }

    public static VariableStatsEstimate computeNegationStatistics(List<VariableStatsEstimate> sourceStats)
    {
        VariableStatsEstimate stats = sourceStats.get(0);
        return VariableStatsEstimate.buildFrom(stats)
                .setLowValue(-stats.getHighValue())
                .setHighValue(-stats.getLowValue())
                .build();
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
                .setAverageRowSize(minExcludingNaNs(getReturnTypeWidth(call, SUM_ARGUMENTS), avgRowSize))
                .build();
    }

    public static VariableStatsEstimate computeHashCodeOperatorStatistics(CallExpression call, List<VariableStatsEstimate> sourceStats, double outputRowCount)
    {
        requireNonNull(call, "call is null");
        checkArgument(sourceStats.size() == 1,
                "exactly one argument expected for hash code operator scalar function");
        VariableStatsEstimate argStats = sourceStats.get(0);
        if (argStats.isUnknown()) {
            return VariableStatsEstimate.unknown();
        }
        VariableStatsEstimate.Builder result =
                VariableStatsEstimate.builder()
                        .setAverageRowSize(minExcludingNaNs(argStats.getAverageRowSize(), getReturnTypeWidth(call, UNKNOWN)))
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
                        .setAverageRowSize(getReturnTypeWidth(call, UNKNOWN))
                        .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                        .setDistinctValuesCount(2.0);
        return result.build();
    }

    public static VariableStatsEstimate computeYearFunctionStatistics(CallExpression call, List<VariableStatsEstimate> sourceStats)
    {
        ISOChronology utcChronology = ISOChronology.getInstanceUTC();
        DateTimeField year = utcChronology.year();

        if (sourceStats.size() != 1 || call.getArguments().size() != 1) {
            return VariableStatsEstimate.unknown();
        }
        VariableStatsEstimate date = sourceStats.get(0);
        VariableStatsEstimate.Builder result = VariableStatsEstimate.builder();
        if (isFinite(date.getLowValue()) && isFinite(date.getHighValue()) && call.getArguments().get(0).getType() instanceof DateType) {
            int minYear = year.get(DAYS.toMillis(Double.valueOf(date.getLowValue()).longValue()));
            int maxYear = year.get(DAYS.toMillis(Double.valueOf(date.getHighValue()).longValue()));
            int ndv = maxYear - minYear + 1;
            result.setDistinctValuesCount(minExcludingNaNs(ndv, date.getDistinctValuesCount()));
            result.setLowValue(minYear);
            result.setHighValue(maxYear);
        }
        result.setAverageRowSize(getReturnTypeWidth(call, UNKNOWN))
                .setNullsFraction(date.getNullsFraction());
        return result.build();
    }
}
