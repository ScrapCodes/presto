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

import com.facebook.presto.FullConnectorSession;
import com.facebook.presto.Session;
import com.facebook.presto.SystemSessionProperties;
import com.facebook.presto.common.function.OperatorType;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.common.type.TypeSignatureParameter;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.function.PropagateSourceStats;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.ExpressionAnalyzer;
import com.facebook.presto.sql.analyzer.Scope;
import com.facebook.presto.sql.planner.ExpressionInterpreter;
import com.facebook.presto.sql.planner.NoOpVariableResolver;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.relational.FunctionResolution;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.sql.tree.ArithmeticBinaryExpression;
import com.facebook.presto.sql.tree.ArithmeticUnaryExpression;
import com.facebook.presto.sql.tree.AstVisitor;
import com.facebook.presto.sql.tree.Cast;
import com.facebook.presto.sql.tree.CoalesceExpression;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.Literal;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.tree.NodeRef;
import com.facebook.presto.sql.tree.NullLiteral;
import com.facebook.presto.sql.tree.SymbolReference;
import com.facebook.presto.type.TypeUtils;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.Map;
import java.util.OptionalDouble;

import static com.facebook.presto.common.function.OperatorType.DIVIDE;
import static com.facebook.presto.common.function.OperatorType.MODULUS;
import static com.facebook.presto.cost.StatsUtil.toStatsRepresentation;
import static com.facebook.presto.spi.relation.ExpressionOptimizer.Level.OPTIMIZED;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.COALESCE;
import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;
import static com.facebook.presto.sql.planner.LiteralInterpreter.evaluate;
import static com.facebook.presto.sql.relational.Expressions.isNull;
import static com.facebook.presto.util.MoreMath.max;
import static com.facebook.presto.util.MoreMath.min;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Double.NaN;
import static java.lang.Double.isFinite;
import static java.lang.Double.isNaN;
import static java.lang.Math.abs;
import static java.lang.String.format;
import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;

public class ScalarStatsCalculator
{
    private final Metadata metadata;

    @Inject
    public ScalarStatsCalculator(Metadata metadata)
    {
        this.metadata = requireNonNull(metadata, "metadata can not be null");
    }

    @Deprecated
    public VariableStatsEstimate calculate(Expression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session, TypeProvider types)
    {
        return new ExpressionStatsVisitor(inputStatistics, session, types).process(scalarExpression);
    }

    public VariableStatsEstimate calculate(RowExpression scalarExpression, PlanNodeStatsEstimate inputStatistics, Session session)
    {
        return scalarExpression.accept(new RowExpressionStatsVisitor(inputStatistics, session.toConnectorSession()), null);
    }

    public VariableStatsEstimate calculate(RowExpression scalarExpression, PlanNodeStatsEstimate inputStatistics, ConnectorSession session)
    {
        return scalarExpression.accept(new RowExpressionStatsVisitor(inputStatistics, session), null);
    }

    private class RowExpressionStatsVisitor
            implements RowExpressionVisitor<VariableStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final ConnectorSession session;
        private final FunctionResolution resolution = new FunctionResolution(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver());

        public RowExpressionStatsVisitor(PlanNodeStatsEstimate input, ConnectorSession session)
        {
            this.input = requireNonNull(input, "input is null");
            this.session = requireNonNull(session, "session is null");
        }

        @Override
        public VariableStatsEstimate visitCall(CallExpression call, Void context)
        {
            if (resolution.isNegateFunction(call.getFunctionHandle())) {
                return computeNegationStatistics(call, context);
            }

            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle());
            if (functionMetadata.getOperatorType().map(OperatorType::isArithmeticOperator).orElse(false)) {
                return computeArithmeticBinaryStatistics(call, context);
            }
            // casting session to FullConnectorSession is not ideal.
            boolean isStatsPropagationEnabled =
                    SystemSessionProperties.shouldEnableScalarFunctionStatsPropagation(((FullConnectorSession) session).getSession());

            if (functionMetadata.getOperatorType().map(OperatorType::isHashOperator).orElse(false) && isStatsPropagationEnabled) {
                return computeHashCodeOperatorStatistics(call, context);
            }

            if (functionMetadata.getOperatorType().map(OperatorType::isComparisonOperator).orElse(false) && isStatsPropagationEnabled) {
                return computeComparisonOperatorStatistics(call, context);
            }

            RowExpression value = new RowExpressionOptimizer(metadata).optimize(call, OPTIMIZED, session);

            if (isNull(value)) {
                return nullStatsEstimate();
            }

            if (value instanceof ConstantExpression) {
                return value.accept(this, context);
            }

            // value is not a constant but we can still propagate estimation through cast
            if (resolution.isCastFunction(call.getFunctionHandle())) {
                return computeCastStatistics(call, context);
            }

            if (call.getDisplayName().equals("concat")) {
                return computeConcatStatistics(call, context);
            }
            if (functionMetadata.getStatsHeader().isPresent() &&
                    isStatsPropagationEnabled) {
                return computeCallStatistics(call, context, functionMetadata.getStatsHeader().get());
            }
            else {
                if (isStatsPropagationEnabled) {
                    System.out.println("Stats not found for func: " + functionMetadata.getName() + " " + call);
                }
            }
            // by default propagate source stats of first col.
            // return propagateCallSourceStatistics(call, context); TODO: add a flag to enable this?
            return VariableStatsEstimate.unknown();
        }

        private VariableStatsEstimate propagateCallSourceStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate sourceStatsFinal = VariableStatsEstimate.unknown();
            for (int i = 0; i < call.getArguments().size(); i++) {
                VariableStatsEstimate sourceStats = call.getArguments().get(i).accept(this, context);
                if (!sourceStats.isUnknown()) {
                    if (sourceStatsFinal.isUnknown()) {
                        sourceStatsFinal = sourceStats;
                        break;
                    }
                }
            }

            return sourceStatsFinal;
        }

        @Override
        public VariableStatsEstimate visitInputReference(InputReferenceExpression reference, Void context)
        {
            throw new UnsupportedOperationException("symbol stats estimation should not reach channel mapping");
        }

        @Override
        public VariableStatsEstimate visitConstant(ConstantExpression literal, Void context)
        {
            if (literal.getValue() == null) {
                return nullStatsEstimate();
            }

            OptionalDouble doubleValue = toStatsRepresentation(metadata.getFunctionAndTypeManager(), session, literal.getType(), literal.getValue());
            VariableStatsEstimate.Builder estimate = VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        public VariableStatsEstimate visitLambda(LambdaDefinitionExpression lambda, Void context)
        {
            return VariableStatsEstimate.unknown();
        }

        @Override
        public VariableStatsEstimate visitVariableReference(VariableReferenceExpression reference, Void context)
        {
            return input.getVariableStatistics(reference);
        }

        @Override
        public VariableStatsEstimate visitSpecialForm(SpecialFormExpression specialForm, Void context)
        {
            if (specialForm.getForm().equals(COALESCE)) {
                VariableStatsEstimate result = null;
                for (RowExpression operand : specialForm.getArguments()) {
                    VariableStatsEstimate operandEstimates = operand.accept(this, context);
                    if (result != null) {
                        result = estimateCoalesce(input, result, operandEstimates);
                    }
                    else {
                        result = operandEstimates;
                    }
                }
                return requireNonNull(result, "result is null");
            }
            return VariableStatsEstimate.unknown();
        }

        private StatisticRange processDistinctValueCountAndRange(CallExpression call, Void context, PropagateSourceStats op)
        {
            StatisticRange s = StatisticRange.empty();
            double ndv = -1;
            for (int i = 0; i < call.getArguments().size(); i++) {
                VariableStatsEstimate sourceStats = call.getArguments().get(i).accept(this, context);
                if (!sourceStats.isUnknown() && isFinite(sourceStats.getDistinctValuesCount())) {
                    switch (op) {
                        case MAX_TYPE_WIDTH:
                            TypeSignature typeSignature = call.getArguments().get(i).getType().getTypeSignature();
                            if (typeSignature.getTypeSignatureBase().hasStandardType() && typeSignature.getTypeSignatureBase().getStandardTypeBase().equals(StandardTypes.VARCHAR)) {
                                for (TypeSignatureParameter t : typeSignature.getParameters()) {
                                    Long longLiteral = t.getLongLiteral();
                                    if (longLiteral > 0) {
                                        ndv = Math.max(ndv, longLiteral);
                                    }
                                }
                            }
                            break;
                        case MAX:
                            s = s.addAndMaxDistinctValues(sourceStats.statisticRange());
                            break;
                        case SUM:
                            s = s.addAndSumDistinctValues(sourceStats.statisticRange());
                            break;
                    }
                }
            }
            if (s.isEmpty() && isFinite(ndv) && ndv > 0.0) {
                s = new StatisticRange(0, ndv, ndv); // This would be the case of MAX_TYPE_WIDTH
            }
            return s;
        }

        private double processNullFraction(CallExpression call, Void context, PropagateSourceStats op)
        {
            double s = 0;
            for (int i = 0; i < call.getArguments().size(); i++) {
                VariableStatsEstimate sourceStats = call.getArguments().get(i).accept(this, context);
                if (!sourceStats.isUnknown() && isFinite(sourceStats.getNullsFraction())) {
                    switch (op) {
                        case MAX:
                            s = max(s, sourceStats.getNullsFraction());
                            break;
                        case SUM:
                            s = s + sourceStats.getNullsFraction();
                            break;
                    }
                }
            }
            if (s > 0.0) {
                return s;
            }
            return NaN;
        }

        private double getReturnTypeWidth(CallExpression call)
        {
            if (call.getType() instanceof VarcharType) {
                VarcharType returnType = (VarcharType) call.getType();
                if (!returnType.isUnbounded()) {
                    return returnType.getLengthSafe();
                }
                else if (call.getDisplayName().equals("concat")) {
                    // since return type is a varchar and length is unknown, if function is concat.
                    // try to get an upper bound.
                    double sum = 0;
                    for (RowExpression r : call.getArguments()) {
                        if (r instanceof CallExpression) {
                            sum += getReturnTypeWidth((CallExpression) r);
                        }
                        if (r.getType() instanceof VarcharType) {
                            VarcharType argType = (VarcharType) r.getType();
                            if (!argType.isUnbounded()) {
                                sum += argType.getLengthSafe();
                            }
                        }
                    }
                    if (sum > 0) {
                        return sum;
                    }
                }
            }
            return NaN;
        }

        private double processAvgRowSize(CallExpression call, Void context, PropagateSourceStats op)
        {
            double s = 0;
            for (int i = 0; i < call.getArguments().size(); i++) {
                VariableStatsEstimate sourceStats = call.getArguments().get(i).accept(this, context);
                if (!sourceStats.isUnknown() && isFinite(sourceStats.getAverageRowSize())) {
                    double s1 = sourceStats.getAverageRowSize();
                    switch (op) {
                        case MAX:
                            s = max(s, s1);
                            break;
                        case SUM:
                            s = s + s1;
                            break;
                    }
                }
            }
            if (s > 0.0) {
                return s;
            }
            return NaN;
        }

        private VariableStatsEstimate computeCallStatistics(CallExpression call, Void context, ScalarStatsHeader statsHeader)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate sourceStatsSum = VariableStatsEstimate.unknown();
            StatisticRange statisticRange = sourceStatsSum.statisticRange();
            double min = sourceStatsSum.getLowValue();
            double max = sourceStatsSum.getHighValue();
            double nullFraction = sourceStatsSum.getNullsFraction();
            double avgRowSize = sourceStatsSum.getAverageRowSize();
            double distinctValuesCount = sourceStatsSum.getDistinctValuesCount();
            // TODO: handle histograms.
            for (Map.Entry<Integer, ScalarPropagateSourceStats> entry : statsHeader.getStatsResolver().entrySet()) {
                ScalarPropagateSourceStats scalarPropagateSourceStats = entry.getValue();
                VariableStatsEstimate sourceStats = call.getArguments().get(entry.getKey()).accept(this, context);
                if (scalarPropagateSourceStats.propagateAllStats() && !sourceStats.isUnknown()) {
                    distinctValuesCount = sourceStats.getDistinctValuesCount();
                    min = sourceStats.getLowValue();
                    max = sourceStats.getHighValue();
                    avgRowSize = sourceStats.getAverageRowSize();
                    nullFraction = sourceStats.getNullsFraction();
                }
                // distinct value count
                switch (scalarPropagateSourceStats.distinctValueCount()) {
                    case SOURCE_STATS:
                        if (isFinite(sourceStats.getDistinctValuesCount())) {
                            distinctValuesCount = sourceStats.getDistinctValuesCount();
                        }
                        break;
                    case ROW_COUNT:
                        distinctValuesCount = input.getOutputRowCount();
                        break;
                    case MAX_TYPE_WIDTH:
                    case MAX:
                    case SUM:
                        statisticRange = processDistinctValueCountAndRange(call, context, scalarPropagateSourceStats.distinctValueCount());
                }
                // min, max can be estimated by distinct value count as well, but user provided hints/values override those.
                switch (scalarPropagateSourceStats.minValue()) {
                    case SOURCE_STATS:
                        if (isFinite(sourceStats.getLowValue())) {
                            min = sourceStats.getLowValue();
                        }
                        break;
                    case MAX:
                    case SUM:
                        throw new UnsupportedOperationException();
                }
                switch (scalarPropagateSourceStats.maxValue()) {
                    case SOURCE_STATS:
                        if (isFinite(sourceStats.getHighValue())) {
                            max = sourceStats.getHighValue();
                        }
                        break;
                    case MAX_TYPE_WIDTH: // Handled as part of distinct value count
                        break;
                    case ROW_COUNT:
                        max = input.getOutputRowCount();
                        break;
                    case MAX:
                    case SUM:
                        throw new UnsupportedOperationException();
                }
                // Average row size
                switch (scalarPropagateSourceStats.avgRowSize()) {
                    case SOURCE_STATS:
                        if (isFinite(sourceStats.getAverageRowSize())) {
                            avgRowSize = sourceStats.getAverageRowSize();
                        }
                        break;
                    case MAX:
                    case SUM:
                        avgRowSize = processAvgRowSize(call, context, scalarPropagateSourceStats.avgRowSize());
                }
                // Null fraction
                switch (scalarPropagateSourceStats.nullFraction()) {
                    case SOURCE_STATS:
                        if (isFinite(sourceStats.getNullsFraction())) {
                            nullFraction = sourceStats.getNullsFraction();
                        }
                        break;
                    case MAX:
                    case SUM:
                        nullFraction = processNullFraction(call, context, scalarPropagateSourceStats.nullFraction());
                }
            }
            // If min and max are set via propagate stats
            if (isFinite(min) && isFinite(max)) {
                statisticRange = new StatisticRange(min, max, distinctValuesCount);
            }
            if (isFinite(getReturnTypeWidth(call)) && !isFinite(avgRowSize)) {
                avgRowSize = getReturnTypeWidth(call);
            }
            // Constant values override any values.
            if (isFinite(statsHeader.getNullFraction())) {
                nullFraction = statsHeader.getNullFraction();
            }
            if (isFinite(statsHeader.getAvgRowSize())) {
                avgRowSize = statsHeader.getAvgRowSize();
            }
            if (isFinite(statsHeader.getDistinctValuesCount())) {
                if (statsHeader.getDistinctValuesCount() == -1.0) {
                    distinctValuesCount = input.getOutputRowCount();
                }
                else {
                    distinctValuesCount = statsHeader.getDistinctValuesCount();
                }
            }
            if (isFinite(statsHeader.getMin()) && isFinite(statsHeader.getMax())) {
                statisticRange = new StatisticRange(statsHeader.getMin(), statsHeader.getMax(), distinctValuesCount);
            }
            sourceStatsSum = VariableStatsEstimate.builder().setStatisticsRange(statisticRange)
                    .setAverageRowSize(avgRowSize)
                    .setNullsFraction(nullFraction)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
            System.out.println("call=" + call + " StatsEstimate=" + sourceStatsSum);
            return sourceStatsSum;
        }

        private VariableStatsEstimate computeConcatStatistics(CallExpression call, Void context)
        {
            double nullFraction = 0.0;
            double ndv = NaN;
            double avgRowSize = 0.0;
            for (RowExpression r : call.getArguments()) {
                VariableStatsEstimate sourceStats = r.accept(this, context);
                if (isFinite(sourceStats.getNullsFraction())) {
                    // concat function returns null if any of the argument is null. So null fraction should add up.
                    nullFraction += sourceStats.getNullsFraction();
                }
                if (isFinite(sourceStats.getDistinctValuesCount())) {
                    ndv = max(ndv, sourceStats.getDistinctValuesCount());
                }
                if (isFinite(sourceStats.getAverageRowSize())) {
                    avgRowSize += sourceStats.getAverageRowSize();
                }
            }
            if (avgRowSize == 0.0) {
                avgRowSize = NaN;
            }
            return VariableStatsEstimate.builder()
                    .setNullsFraction(nullFraction)
                    .setDistinctValuesCount(min(ndv, input.getOutputRowCount()))
                    .setAverageRowSize(min(getReturnTypeWidth(call), avgRowSize))
                    .build();
        }

        private VariableStatsEstimate computeCastStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate sourceStats = call.getArguments().get(0).accept(this, context);

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (TypeUtils.isIntegralType(call.getType().getTypeSignature(), metadata.getFunctionAndTypeManager())) {
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
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        private VariableStatsEstimate computeNegationStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate stats = call.getArguments().get(0).accept(this, context);
            if (resolution.isNegateFunction(call.getFunctionHandle())) {
                return VariableStatsEstimate.buildFrom(stats)
                        .setLowValue(-stats.getHighValue())
                        .setHighValue(-stats.getLowValue())
                        .build();
            }
            throw new IllegalStateException(format("Unexpected sign: %s(%s)", call.getDisplayName(), call.getFunctionHandle()));
        }

        private VariableStatsEstimate computeHashCodeOperatorStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate argStats = call.getArguments().get(0).accept(this, context);
            VariableStatsEstimate.Builder result =
                    VariableStatsEstimate.builder()
                            .setAverageRowSize(8.0)
                            .setNullsFraction(argStats.getNullsFraction())
                            .setDistinctValuesCount(min(argStats.getDistinctValuesCount(), input.getOutputRowCount()));
            return result.build();
        }

        private VariableStatsEstimate computeComparisonOperatorStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate argStats = call.getArguments().get(0).accept(this, context);
            VariableStatsEstimate.Builder result =
                    VariableStatsEstimate.builder()
                            .setAverageRowSize(1.0)
                            .setNullsFraction(0.0)
                            .setLowValue(0.0)
                            .setHighValue(1.0)
                            .setDistinctValuesCount(2.0);
            return result.build();
        }

        private VariableStatsEstimate computeArithmeticBinaryStatistics(CallExpression call, Void context)
        {
            requireNonNull(call, "call is null");
            VariableStatsEstimate left = call.getArguments().get(0).accept(this, context);
            VariableStatsEstimate right = call.getArguments().get(1).accept(this, context);

            VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                    .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            FunctionMetadata functionMetadata = metadata.getFunctionAndTypeManager().getFunctionMetadata(call.getFunctionHandle());
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

        private double operate(OperatorType operator, double left, double right)
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
    }

    private class ExpressionStatsVisitor
            extends AstVisitor<VariableStatsEstimate, Void>
    {
        private final PlanNodeStatsEstimate input;
        private final Session session;
        private final TypeProvider types;

        ExpressionStatsVisitor(PlanNodeStatsEstimate input, Session session, TypeProvider types)
        {
            this.input = input;
            this.session = session;
            this.types = types;
        }

        @Override
        protected VariableStatsEstimate visitNode(Node node, Void context)
        {
            return VariableStatsEstimate.unknown();
        }

        @Override
        protected VariableStatsEstimate visitSymbolReference(SymbolReference node, Void context)
        {
            return input.getVariableStatistics(new VariableReferenceExpression(getSourceLocation(node), node.getName(), types.get(node)));
        }

        @Override
        protected VariableStatsEstimate visitNullLiteral(NullLiteral node, Void context)
        {
            return nullStatsEstimate();
        }

        @Override
        protected VariableStatsEstimate visitLiteral(Literal node, Void context)
        {
            Object value = evaluate(metadata, session.toConnectorSession(), node);
            Type type = ExpressionAnalyzer.createConstantAnalyzer(metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(), session, ImmutableMap.of(), WarningCollector.NOOP).analyze(node, Scope.create());
            OptionalDouble doubleValue = toStatsRepresentation(metadata, session, type, value);
            VariableStatsEstimate.Builder estimate = VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1);

            if (doubleValue.isPresent()) {
                estimate.setLowValue(doubleValue.getAsDouble());
                estimate.setHighValue(doubleValue.getAsDouble());
            }
            return estimate.build();
        }

        @Override
        protected VariableStatsEstimate visitFunctionCall(FunctionCall node, Void context)
        {
            Map<NodeRef<Expression>, Type> expressionTypes = getExpressionTypes(session, node, types);
            ExpressionInterpreter interpreter = ExpressionInterpreter.expressionOptimizer(node, metadata, session, expressionTypes);
            Object value = interpreter.optimize(NoOpVariableResolver.INSTANCE);

            if (value == null || value instanceof NullLiteral) {
                return nullStatsEstimate();
            }

            if (value instanceof Expression && !(value instanceof Literal)) {
                // value is not a constant
                return VariableStatsEstimate.unknown();
            }

            // value is a constant
            return VariableStatsEstimate.builder()
                    .setNullsFraction(0)
                    .setDistinctValuesCount(1)
                    .build();
        }

        private Map<NodeRef<Expression>, Type> getExpressionTypes(Session session, Expression expression, TypeProvider types)
        {
            ExpressionAnalyzer expressionAnalyzer = ExpressionAnalyzer.createWithoutSubqueries(
                    metadata.getFunctionAndTypeManager().getFunctionAndTypeResolver(),
                    session,
                    types,
                    emptyMap(),
                    node -> new IllegalStateException("Unexpected node: " + node),
                    WarningCollector.NOOP,
                    false);
            expressionAnalyzer.analyze(expression, Scope.create());
            return expressionAnalyzer.getExpressionTypes();
        }

        @Override
        protected VariableStatsEstimate visitCast(Cast node, Void context)
        {
            VariableStatsEstimate sourceStats = process(node.getExpression());
            TypeSignature targetType = TypeSignature.parseTypeSignature(node.getType());

            // todo - make this general postprocessing rule.
            double distinctValuesCount = sourceStats.getDistinctValuesCount();
            double lowValue = sourceStats.getLowValue();
            double highValue = sourceStats.getHighValue();

            if (TypeUtils.isIntegralType(targetType, metadata.getFunctionAndTypeManager())) {
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
                    .setNullsFraction(sourceStats.getNullsFraction())
                    .setLowValue(lowValue)
                    .setHighValue(highValue)
                    .setDistinctValuesCount(distinctValuesCount)
                    .build();
        }

        @Override
        protected VariableStatsEstimate visitArithmeticUnary(ArithmeticUnaryExpression node, Void context)
        {
            VariableStatsEstimate stats = process(node.getValue());
            switch (node.getSign()) {
                case PLUS:
                    return stats;
                case MINUS:
                    return VariableStatsEstimate.buildFrom(stats)
                            .setLowValue(-stats.getHighValue())
                            .setHighValue(-stats.getLowValue())
                            .build();
                default:
                    throw new IllegalStateException("Unexpected sign: " + node.getSign());
            }
        }

        @Override
        protected VariableStatsEstimate visitArithmeticBinary(ArithmeticBinaryExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            VariableStatsEstimate left = process(node.getLeft());
            VariableStatsEstimate right = process(node.getRight());

            VariableStatsEstimate.Builder result = VariableStatsEstimate.builder()
                    .setAverageRowSize(Math.max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .setNullsFraction(left.getNullsFraction() + right.getNullsFraction() - left.getNullsFraction() * right.getNullsFraction())
                    .setDistinctValuesCount(min(left.getDistinctValuesCount() * right.getDistinctValuesCount(), input.getOutputRowCount()));

            double leftLow = left.getLowValue();
            double leftHigh = left.getHighValue();
            double rightLow = right.getLowValue();
            double rightHigh = right.getHighValue();
            if (isNaN(leftLow) || isNaN(leftHigh) || isNaN(rightLow) || isNaN(rightHigh)) {
                result.setLowValue(NaN)
                        .setHighValue(NaN);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.DIVIDE && rightLow < 0 && rightHigh > 0) {
                result.setLowValue(Double.NEGATIVE_INFINITY)
                        .setHighValue(Double.POSITIVE_INFINITY);
            }
            else if (node.getOperator() == ArithmeticBinaryExpression.Operator.MODULUS) {
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
                double v1 = operate(node.getOperator(), leftLow, rightLow);
                double v2 = operate(node.getOperator(), leftLow, rightHigh);
                double v3 = operate(node.getOperator(), leftHigh, rightLow);
                double v4 = operate(node.getOperator(), leftHigh, rightHigh);
                double lowValue = min(v1, v2, v3, v4);
                double highValue = max(v1, v2, v3, v4);

                result.setLowValue(lowValue)
                        .setHighValue(highValue);
            }

            return result.build();
        }

        private double operate(ArithmeticBinaryExpression.Operator operator, double left, double right)
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

        @Override
        protected VariableStatsEstimate visitCoalesceExpression(CoalesceExpression node, Void context)
        {
            requireNonNull(node, "node is null");
            VariableStatsEstimate result = null;
            for (Expression operand : node.getOperands()) {
                VariableStatsEstimate operandEstimates = process(operand);
                if (result != null) {
                    result = estimateCoalesce(input, result, operandEstimates);
                }
                else {
                    result = operandEstimates;
                }
            }
            return requireNonNull(result, "result is null");
        }
    }

    private static VariableStatsEstimate estimateCoalesce(PlanNodeStatsEstimate input, VariableStatsEstimate left, VariableStatsEstimate right)
    {
        // Question to reviewer: do you have a method to check if fraction is empty or saturated?
        if (left.getNullsFraction() == 0) {
            return left;
        }
        else if (left.getNullsFraction() == 1.0) {
            return right;
        }
        else {
            return VariableStatsEstimate.builder()
                    .setLowValue(min(left.getLowValue(), right.getLowValue()))
                    .setHighValue(max(left.getHighValue(), right.getHighValue()))
                    .setDistinctValuesCount(left.getDistinctValuesCount() +
                            min(right.getDistinctValuesCount(), input.getOutputRowCount() * left.getNullsFraction()))
                    .setNullsFraction(left.getNullsFraction() * right.getNullsFraction())
                    // TODO check if dataSize estimation method is correct
                    .setAverageRowSize(max(left.getAverageRowSize(), right.getAverageRowSize()))
                    .build();
        }
    }

    private static VariableStatsEstimate nullStatsEstimate()
    {
        return VariableStatsEstimate.builder()
                .setDistinctValuesCount(0)
                .setNullsFraction(1)
                .build();
    }
}
