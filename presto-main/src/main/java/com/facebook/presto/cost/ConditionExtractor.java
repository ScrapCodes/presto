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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.plan.EquiJoinClause;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.Condition;
import com.facebook.presto.spi.relation.FilterConditionUnresolved;
import com.facebook.presto.spi.relation.JoinConditionUnresolved;
import com.facebook.presto.spi.relation.UnresolvedCondition;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.SimplePlanVisitor;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;
import com.facebook.presto.sql.planner.plan.JoinNode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;

public class ConditionExtractor
{
    private final Lookup lookup;
    private final PlanNode plan;

    ConditionExtractor(PlanNode plan, Lookup lookup)
    {
        this.lookup = lookup;
        this.plan = plan;
    }

    public List<Condition> extractConditions()
    {
        // todo: it should support both Join and Filter PlanNode
        Context context = new Context();
        try {
            this.plan.accept(new Visitor(this.lookup), context);
            return context.getConditions();
        }
        catch (UnsupportedOperationException | IllegalStateException e) {
            return null;
        }
    }

    public static class Context
    {
        List<Condition> conditions = new ArrayList<>();
        Map<VariableReferenceExpression, String> aliasToColumnMap = new HashMap<>();
        List<UnresolvedCondition> unresolvedConditions = new ArrayList<>();

        public List<Condition> getConditions()
        {
            return conditions;
        }

        public Map<VariableReferenceExpression, String> getAliasToColumnMap()
        {
            return aliasToColumnMap;
        }

        public List<UnresolvedCondition> getUnresolvedConditions()
        {
            return unresolvedConditions;
        }
//
//        public List<FilterConditionUnresolved> getUnresolvedFilterConditions()
//        {
//            return unresolvedFilterConditions;
//        }

        public void resolveStoredConditions()
        {
            List<UnresolvedCondition> resolvedConditions = new ArrayList<>();
            for (UnresolvedCondition unresolved : unresolvedConditions) {
                Condition condition = unresolved.resolveAlias((aliasToColumnMap));
                if (condition != null) {
                    resolvedConditions.add(unresolved);
                    conditions.add(condition);
                }
            }
            unresolvedConditions.removeAll(resolvedConditions);
        }
    }

    private static class Visitor
            extends SimplePlanVisitor<Context>
    {
        private final Lookup lookup;

        private Visitor(Lookup lookup)
        {
            this.lookup = lookup;
        }

        private String resolveTableColumn(String table, ColumnHandle columnHandle)
        {
            return table + "." + columnHandle.getName();
        }

        @Override
        public Void visitPlan(PlanNode node, Context context)
        {
            if (node instanceof GroupReference) {
                return visitGroupReference((GroupReference) node, context);
            }
            return super.visitPlan(node, context);
        }

        @Override
        public Void visitJoin(JoinNode node, Context context)
        {
            // Extract join conditions
            List<EquiJoinClause> joinClauses = node.getCriteria();

            for (EquiJoinClause clause : joinClauses) {
                VariableReferenceExpression left = clause.getLeft();
                VariableReferenceExpression right = clause.getRight();
                // Store unresolved conditions
                context.getUnresolvedConditions().add(new JoinConditionUnresolved(left, right));
            }

            // Traverse child nodes first to collect alias information
            node.getLeft().accept(this, context);
            node.getRight().accept(this, context);

            // Resolve the stored conditions after traversing parent nodes to collect alias information
            context.resolveStoredConditions();
            return null;
        }

        @Override
        public Void visitTableScan(TableScanNode node, Context context)
        {
            Map<VariableReferenceExpression, ColumnHandle> assignments = node.getAssignments();
            String table = node.getTable().getConnectorHandle().getTableName().toString();
            for (Map.Entry<VariableReferenceExpression, ColumnHandle> entry : assignments.entrySet()) {
                VariableReferenceExpression variable = entry.getKey();
                ColumnHandle columnHandle = entry.getValue();
                // todo: add a function to get table and col real name from the columnHandle.
                String tableColumn = resolveTableColumn(table, columnHandle);
                // Map alias to real table column
                context.getAliasToColumnMap().put(variable, tableColumn);
            }
            return null;
        }

        @Override
        public Void visitFilter(FilterNode node, Context context)
        {
            // todo: may need to check groupreference type
            // transfer the filter RowExpression to Condition
            assert (node.getSource() instanceof TableScanNode) ||
                    (node.getSource() instanceof GroupReference && resolveGroupReferences(node.getSource(), lookup) instanceof TableScanNode);
//            else {
//                throw new UnsupportedOperationException(node.getClass().getSimpleName() + "is unsupported if parent node is not TableScan");
//            }
            context.getUnresolvedConditions().add(new FilterConditionUnresolved(node.getPredicate()));
            super.visitPlan(node, context);
            context.resolveStoredConditions();
            return null;
        }

        @Override
        public Void visitGroupReference(GroupReference node, Context context)
        {
            // Resolve the actual PlanNode using the groupId
//            Memo memo = this.memo.orElseThrow(() -> new IllegalStateException("ML without memo cannot handle GroupReferences"));
            PlanNode referencedPlanNode = resolveGroupReferences(node, this.lookup);

            // Handle the resolved PlanNode as needed
            if (referencedPlanNode != null) {
                referencedPlanNode.accept(this, context);  // Visit the resolved PlanNode
            }
            else {
                System.out.println("Warning: No PlanNode found for GroupId " + node.getGroupId());
            }

            return null;  // Return null or handle as needed
        }

        // todo: for all other types of nodes, either return null or return unsupported
//        @Override
//        public Void visitAggregation(AggregationNode node, Context context) {
//            throw new UnsupportedOperationException();
//        }
    }
}
