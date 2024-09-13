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

import com.facebook.presto.Session;
import com.facebook.presto.matching.Pattern;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.relation.Condition;
import com.facebook.presto.spi.statistics.MLBasedSourceInfo;
import com.facebook.presto.spi.statistics.SourceInfo;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.iterative.GroupReference;
import com.facebook.presto.sql.planner.iterative.Lookup;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.SystemSessionProperties.isDefaultFilterFactorEnabled;
import static com.facebook.presto.SystemSessionProperties.useMLBasedStatisticsEnabled;
import static com.facebook.presto.cost.FilterStatsCalculator.UNKNOWN_FILTER_COEFFICIENT;
import static com.facebook.presto.sql.planner.iterative.Plans.resolveGroupReferences;
import static com.facebook.presto.sql.planner.plan.Patterns.filter;

public class FilterStatsRule
        extends SimpleStatsRule<FilterNode>
{
    private static final Pattern<FilterNode> PATTERN = filter();

    private final FilterStatsCalculator filterStatsCalculator;

    public FilterStatsRule(StatsNormalizer normalizer, FilterStatsCalculator filterStatsCalculator)
    {
        super(normalizer);
        this.filterStatsCalculator = filterStatsCalculator;
    }

    @Override
    public Pattern<FilterNode> getPattern()
    {
        return PATTERN;
    }

    @Override
    public Optional<PlanNodeStatsEstimate> doCalculate(FilterNode node, StatsProvider statsProvider, Lookup lookup, Session session, TypeProvider types)
    {
        // if the source node is a TableScan node, then directly call the Python code to get an estimate of Filter
        // needs to transfer the filter conditions to strings and get the table name
        PlanNodeStatsEstimate estimate = PlanNodeStatsEstimate.unknown();
        if (useMLBasedStatisticsEnabled(session)) {
            if ((node.getSource() instanceof TableScanNode) || (node.getSource() instanceof GroupReference && resolveGroupReferences(node.getSource(), lookup) instanceof TableScanNode)) {
//                String table = (((TableScanNode) node.getSource()).getTable().getConnectorHandle()).getTableName().toString();
                ConditionExtractor conditionExtractor = new ConditionExtractor(node, lookup);
                List<Condition> conditions = conditionExtractor.extractConditions();
                if (conditions != null) {
                    // todo: cause Filter is always pushed down, there should only be one filter condition without join conditions
                    assert conditions.size() == 1;
                    estimate = filterStatsCalculator.filterStatsUsingML(conditions.get(0), session.getMlStatsMap());
                }
            }
        }

        if (!estimate.isOutputRowCountUnknown()) {
            estimate.setSourceInfo(new MLBasedSourceInfo(SourceInfo.ConfidenceLevel.HIGH));
//            System.out.println(estimate.getOutputRowCount());
            return Optional.of(estimate);
        }
        PlanNodeStatsEstimate sourceStats = statsProvider.getStats(node.getSource());
        estimate = filterStatsCalculator.filterStats(sourceStats, node.getPredicate(), session);

        if (isDefaultFilterFactorEnabled(session) && estimate.isOutputRowCountUnknown()) {
            estimate = sourceStats.mapOutputRowCount(sourceRowCount -> sourceStats.getOutputRowCount() * UNKNOWN_FILTER_COEFFICIENT);
        }
        return Optional.of(estimate);
    }
}
