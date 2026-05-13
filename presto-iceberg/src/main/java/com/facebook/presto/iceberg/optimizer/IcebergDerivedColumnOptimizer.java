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
package com.facebook.presto.iceberg.optimizer;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.BaseHiveColumnHandle;
import com.facebook.presto.iceberg.ColumnIdentity;
import com.facebook.presto.iceberg.IcebergAbstractMetadata;
import com.facebook.presto.iceberg.IcebergColumnHandle;
import com.facebook.presto.iceberg.IcebergTableHandle;
import com.facebook.presto.iceberg.IcebergTableLayoutHandle;
import com.facebook.presto.iceberg.IcebergTableProperties;
import com.facebook.presto.iceberg.IcebergUtil;
import com.facebook.presto.iceberg.derivedColumn.DerivedColumnArgumentSpec;
import com.facebook.presto.iceberg.derivedColumn.DerivedColumnRef;
import com.facebook.presto.iceberg.derivedColumn.DerivedColumnUDFSpec;
import com.facebook.presto.iceberg.transaction.IcebergTransactionManager;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorPlanOptimizer;
import com.facebook.presto.spi.ConnectorPlanRewriter;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.TableHandle;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.FunctionMetadataManager;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.PlanNodeIdAllocator;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.base.Joiner;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Multimap;
import io.airlift.slice.Slice;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.iceberg.IcebergSessionProperties.isDerivedColumnsEnabled;
import static com.facebook.presto.iceberg.IcebergTableType.DATA;
import static com.facebook.presto.spi.ConnectorPlanRewriter.rewriteWith;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public class IcebergDerivedColumnOptimizer
        implements ConnectorPlanOptimizer
{
    private static final Logger LOG = Logger.get(IcebergDerivedColumnOptimizer.class);
    private final IcebergTableProperties tableProperties;
    private final IcebergTransactionManager transactionManager;
    private StandardFunctionResolution functionResolution;
    private TypeManager typeManager;
    private FunctionMetadataManager functionMetadataManager;

    public IcebergDerivedColumnOptimizer(
            IcebergTableProperties tableProperties,
            IcebergTransactionManager transactionManager,
            StandardFunctionResolution functionResolution,
            TypeManager typeManager,
            FunctionMetadataManager functionMetadataManager)
    {
        this.tableProperties = tableProperties;
        this.transactionManager = transactionManager;
        this.functionResolution = functionResolution;
        this.typeManager = typeManager;
        this.functionMetadataManager = functionMetadataManager;
    }

    @Override
    public PlanNode optimize(PlanNode maxSubplan, ConnectorSession session, VariableAllocator variableAllocator, PlanNodeIdAllocator idAllocator)
    {
        return rewriteWith(new DerivedColumnRewriter(tableProperties, functionResolution, typeManager, functionMetadataManager,
                transactionManager, idAllocator, session), maxSubplan);
    }

    private record RewrittenFilter(TableScanNode tableScanNode, RowExpression filter) {}

    private static class DerivedColumnRewriter
            extends ConnectorPlanRewriter<Void>
    {
        private final ConnectorSession session;
        private final TypeManager typeManager;
        private final IcebergTableProperties tableProperties1;
        private final StandardFunctionResolution functionResolution;
        private final FunctionMetadataManager functionMetadataManager;
        private final PlanNodeIdAllocator idAllocator;
        private final IcebergTransactionManager transactionManager;

        public DerivedColumnRewriter(
                IcebergTableProperties tableProperties,
                StandardFunctionResolution functionResolution,
                TypeManager typeManager,
                FunctionMetadataManager functionMetadataManager,
                IcebergTransactionManager transactionManager,
                PlanNodeIdAllocator idAllocator,
                ConnectorSession session)
        {
            this.tableProperties1 = tableProperties;
            this.functionResolution = functionResolution;
            this.typeManager = typeManager;
            this.functionMetadataManager = functionMetadataManager;
            this.transactionManager = transactionManager;
            this.idAllocator = idAllocator;
            this.session = session;
        }

        @Override
        public PlanNode visitFilter(FilterNode filter, ConnectorPlanRewriter.RewriteContext<Void> context)
        {
            if (!isDerivedColumnsEnabled(session)) {
                return filter;
            }

            if (!(filter.getSource() instanceof TableScanNode)) {
                return visitPlan(filter, context);
            }

            TableScanNode tableScan = (TableScanNode) filter.getSource();
            if (((IcebergTableHandle) tableScan.getTable().getConnectorHandle()).getIcebergTableName().getTableType() != DATA) {
                return visitPlan(filter, context);
            }
            TableHandle handle = tableScan.getTable();
            IcebergAbstractMetadata metadata = (IcebergAbstractMetadata) transactionManager.get(handle.getTransaction());
            IcebergTableHandle tableHandle = (IcebergTableHandle) handle.getConnectorHandle();
            ConnectorTableMetadata tableMetadata = metadata.getTableMetadata(session, tableHandle);
            Map<String, Integer> columnIndexMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                    .collect(Collectors.toMap(ColumnMetadata::getName, col ->
                            IcebergUtil.getIcebergTable(metadata, session,
                                    ((IcebergTableHandle) tableScan.getTable().getConnectorHandle()).getSchemaTableName()).schema().findField(col.getName()).fieldId()));
            List<String> derivedColumns = tableProperties1.getDerivedColumns(tableMetadata.getProperties());
            if (derivedColumns.isEmpty()) {
                return filter;
            }
            Map<String, ColumnMetadata> columnsMap = tableMetadata.getColumns().stream().filter(col -> !col.isHidden())
                    .collect(Collectors.toMap(ColumnMetadata::getName, col -> col));
            checkState(columnsMap.keySet().containsAll(derivedColumns),
                    format("Incorrect derived column definition, configured derived columns: %s does not exist in table: %s", Joiner.on(',').join(derivedColumns),
                            tableHandle.getIcebergTableName()));

            List<DerivedColumnUDFSpec> derivedColumnUDFSpecs = tableProperties1.getDerivedColumnUDFSpec(tableMetadata.getProperties()).getUdfSpecList();
            Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap = ArrayListMultimap.create();

            derivedColumnUDFSpecs.forEach(udfSpec -> {
                FunctionHandle functionHandle = functionResolution.lookupFunction(
                        udfSpec.getCatalog(),
                        udfSpec.getSchema(),
                        udfSpec.getFunctionName(),
                        udfSpec.getParameterTypes().stream().map(type -> typeManager.getType(parseTypeSignature(type))).toList());
                derivedColumnUDFSpecMap.put(functionHandle.canonicalize(), udfSpec);
            });

            RowExpression filterPredicate = filter.getPredicate();
            RewrittenFilter rewrittenFilter = rewriteFilter(tableScan, filterPredicate, derivedColumnUDFSpecMap, columnsMap, columnIndexMap);
            return new FilterNode(filter.getSourceLocation(), idAllocator.getNextId(), filter.getStatsEquivalentPlanNode(), rewrittenFilter.tableScanNode, rewrittenFilter.filter);
        }

        private RewrittenFilter rewriteFilter(
                TableScanNode tableScan,
                RowExpression filterPredicate,
                Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap,
                Map<String, ColumnMetadata> columnsMap,
                Map<String, Integer> columnIndexMap)
        {
            RowExpression filterPredicateRewritten;
            Set<VariableReferenceExpression> outputVariables = new HashSet<>(tableScan.getOutputVariables());
            Map<VariableReferenceExpression, ColumnHandle> tableAssignments = new HashMap<>(tableScan.getAssignments());
            TableHandle handle = tableScan.getTable();
            RewrittenRowExp rewrittenCallExp = filterPredicate.accept(new DerivedColumnRewriteCallExpression(), new RewriteContext(derivedColumnUDFSpecMap, columnsMap));
            filterPredicateRewritten = rewrittenCallExp.rewrittenPredicate;
            Function<VariableReferenceExpression, IcebergColumnHandle> derivedColumnHandle = varRef -> new IcebergColumnHandle(
                    new ColumnIdentity(columnIndexMap.get(varRef.getName()), varRef.getName(), ColumnIdentity.TypeCategory.PRIMITIVE, List.of()),
                    columnsMap.get(varRef.getName()).getType(),
                    Optional.of("derived column"),
                    BaseHiveColumnHandle.ColumnType.REGULAR);
            if (!outputVariables.containsAll(rewrittenCallExp.derivedColumns)) {
                outputVariables.addAll(rewrittenCallExp.derivedColumns);
                tableAssignments.putAll(rewrittenCallExp.derivedColumns.stream()
                        .collect(Collectors.toMap(k -> k, derivedColumnHandle)));
            }
            Optional<ConnectorTableLayoutHandle> newConnectorTableLayoutHandle = handle.getLayout().map(IcebergTableLayoutHandle.class::cast)
                    .map(icebergTableLayoutHandle -> new IcebergTableLayoutHandle(
                            icebergTableLayoutHandle.getPartitionColumns().stream()
                                    .map(IcebergColumnHandle.class::cast).collect(toList()),
                            icebergTableLayoutHandle.getDataColumns(),
                            icebergTableLayoutHandle.getDomainPredicate(),
                            icebergTableLayoutHandle.getRemainingPredicate(),
                            icebergTableLayoutHandle.getPredicateColumns(),
                            Optional.of(ImmutableSet.<IcebergColumnHandle>builder().addAll(icebergTableLayoutHandle.getRequestedColumns().orElse(ImmutableSet.of()))
                                    .addAll(rewrittenCallExp.derivedColumns.stream().map(derivedColumnHandle).collect(Collectors.toSet())).build()),
                            icebergTableLayoutHandle.isPushdownFilterEnabled(),
                            icebergTableLayoutHandle.getPartitionColumnPredicate(),
                            icebergTableLayoutHandle.getPartitions(),
                            icebergTableLayoutHandle.getTable()));

            TableScanNode newTableScan = new TableScanNode(
                    tableScan.getSourceLocation(),
                    tableScan.getId(),
                    new TableHandle(handle.getConnectorId(), handle.getConnectorHandle(), handle.getTransaction(), newConnectorTableLayoutHandle),
                    outputVariables.stream().toList(),
                    tableAssignments,
                    tableScan.getCurrentConstraint(),
                    tableScan.getEnforcedConstraint(),
                    tableScan.getCteMaterializationInfo());
            return new RewrittenFilter(newTableScan, filterPredicateRewritten);
        }

        private record RewrittenRowExp(RowExpression rewrittenPredicate, Set<VariableReferenceExpression> derivedColumns) {}

        private record RewriteContext(Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap, Map<String, ColumnMetadata> columnMetadataMap) {}

        private static class DerivedColumnRewriteCallExpression
                implements RowExpressionVisitor<RewrittenRowExp, RewriteContext>
        {
            @Override
            public RewrittenRowExp visitExpression(RowExpression expression, RewriteContext context)
            {
                return new RewrittenRowExp(expression, ImmutableSet.of());
            }

            @Override
            public RewrittenRowExp visitSpecialForm(SpecialFormExpression specialForm, RewriteContext context)
            {
                Set<VariableReferenceExpression> derivedColumnsAdded = new HashSet<>();
                List<RewrittenRowExp> arguments = specialForm.getArguments().stream().map(x -> x.accept(this, context)).toList();
                arguments.forEach(arg -> derivedColumnsAdded.addAll(arg.derivedColumns));
                return new RewrittenRowExp(new SpecialFormExpression(specialForm.getSourceLocation(), specialForm.getForm(), specialForm.getType(),
                        arguments.stream().map(arg -> arg.rewrittenPredicate).toList()), derivedColumnsAdded);
            }

            @Override
            public RewrittenRowExp visitCall(CallExpression filterPredicate, RewriteContext rewriteContext)
            {
                Set<VariableReferenceExpression> derivedColumnsAdded = new HashSet<>();
                List<RewrittenRowExp> arguments = filterPredicate.getArguments().stream().map(x -> x.accept(this, rewriteContext)).toList();
                arguments.forEach(arg -> derivedColumnsAdded.addAll(arg.derivedColumns));
                List<RowExpression> argumentsRowExpression = arguments.stream().map(arg -> arg.rewrittenPredicate).toList();
                FunctionHandle functionHandleArg = filterPredicate.getFunctionHandle();
                Multimap<FunctionHandle, DerivedColumnUDFSpec> derivedColumnUDFSpecMap = rewriteContext.derivedColumnUDFSpecMap;
                FunctionHandle functionHandle = functionHandleArg.canonicalize();
                if (derivedColumnUDFSpecMap.containsKey(functionHandle)) { // Possible match !
                    Collection<DerivedColumnUDFSpec> derivedColumnUDFSpecs = derivedColumnUDFSpecMap.get(functionHandle);
                    List<DerivedColumnArgumentSpec> argumentSpecList = getDerivedColumnArgumentSpecs(argumentsRowExpression);
                    // Next we search for a derived column spec, which exactly matches (including arguments') the call expression (i.e. UDF)
                    Set<DerivedColumnUDFSpec> matchingUDFSpec =
                            derivedColumnUDFSpecs.stream().filter(derivedColumnSpec ->
                                    matchTwoArgumentsList(derivedColumnSpec.getArguments(), argumentSpecList)).collect(Collectors.toSet());
                    // We can either have a exact match or no match, if we get more than one match - that indicates duplicate(redundant) entries in udf spec.
                    checkState(matchingUDFSpec.size() < 2,
                            format("derived-columns: A duplicate UDF configuration found in udf specs, for : %s ", Joiner.on(",").join(matchingUDFSpec)));
                    if (!matchingUDFSpec.isEmpty()) {
                        // Finally swap call expression with variable ref exp, i.e. UDF -> derived column.
                        DerivedColumnUDFSpec derivedColumnUDFSpec = matchingUDFSpec.stream().findFirst().get();
                        String derivedColumnName = derivedColumnUDFSpec.getDerivedColumnName();
                        ColumnMetadata derivedColumnMetadata = rewriteContext.columnMetadataMap.get(derivedColumnName);
                        VariableReferenceExpression derivedColumn =
                                new VariableReferenceExpression(Optional.empty(), derivedColumnName, derivedColumnMetadata.getType());
                        derivedColumnsAdded.add(derivedColumn);
                        return new RewrittenRowExp(derivedColumn, derivedColumnsAdded);
                    }
                }
                CallExpression predicate =
                        new CallExpression(filterPredicate.getDisplayName(), filterPredicate.getFunctionHandle(), filterPredicate.getType(), argumentsRowExpression);
                return new RewrittenRowExp(predicate, derivedColumnsAdded);
            }

            private Boolean matchTwoArgumentsList(List<DerivedColumnArgumentSpec> list1, List<DerivedColumnArgumentSpec> list2)
            {
                if (list1.size() != list2.size()) {
                    return false;
                }
                for (int i = 0; i < list1.size(); i++) {
                    if (!list1.get(i).equals(list2.get(i))) {
                        return false;
                    }
                }
                return true;
            }

            private List<DerivedColumnArgumentSpec> getDerivedColumnArgumentSpecs(List<RowExpression> arguments)
            {
                // reconstruct the call expressions' arguments as DerivedColumnArgumentSpec for matching
                List<DerivedColumnArgumentSpec> argumentSpecList = new ArrayList<>();
                Integer index = 0;
                for (RowExpression arg : arguments) {
                    if (arg instanceof VariableReferenceExpression) {
                        VariableReferenceExpression variableReferenceExpression = (VariableReferenceExpression) arg;
                        argumentSpecList.add(new DerivedColumnArgumentSpec(index, variableReferenceExpression.getType().getTypeSignature().getBase(),
                                variableReferenceExpression.getName(), DerivedColumnRef.COLUMN));
                    }
                    else if (arg instanceof ConstantExpression) {
                        ConstantExpression constantExpression = (ConstantExpression) arg;
                        argumentSpecList.add(new DerivedColumnArgumentSpec(index, constantExpression.getType().getTypeSignature().getBase(),
                                getConstantExpressionValue(constantExpression), DerivedColumnRef.CONSTANT));
                    }
                    else {
                        return ImmutableList.of();
                    }
                    index++;
                }
                return argumentSpecList;
            }
        }

        private static String getConstantExpressionValue(ConstantExpression constantExpression)
        {
            if (constantExpression.getValue() instanceof Slice) {
                return ((Slice) constantExpression.getValue()).toStringUtf8();
            }
            return constantExpression.getValue().toString();
        }
    }
}
