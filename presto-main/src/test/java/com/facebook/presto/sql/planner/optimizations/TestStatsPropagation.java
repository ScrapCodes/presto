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

package com.facebook.presto.sql.planner.optimizations;

import com.facebook.presto.cost.PlanNodeStatsEstimate;
import com.facebook.presto.cost.VariableStatsEstimate;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.sql.Optimizer;
import com.facebook.presto.sql.planner.Plan;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.testing.LocalQueryRunner;
import com.facebook.presto.testing.TestngUtils;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.util.List;
import java.util.function.Predicate;

import static com.facebook.presto.SystemSessionProperties.SCALAR_FUNCTION_STATS_PROPAGATION_ENABLED;
import static java.lang.Double.isFinite;
import static org.testng.Assert.assertTrue;

public class TestStatsPropagation
        extends BasePlanTest
{
    private LocalQueryRunner queryRunner;

    private void assertPlanHasExpectedStats(Predicate<PlanNodeStatsEstimate> statsChecker, @Language("SQL") String sql)
    {
        List<PlanOptimizer> optimizers = queryRunner.getPlanOptimizers(true);
        queryRunner.inTransaction(queryRunner.getDefaultSession(), transactionSession -> {
            Plan actualPlanResult = queryRunner.createPlan(
                    transactionSession,
                    sql,
                    optimizers,
                    Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED,
                    WarningCollector.NOOP);

            assertTrue(actualPlanResult.getStatsAndCosts().getStats().values().stream().allMatch(statsChecker), sql);
            return null;
        });
    }

    private void assertPlanHasExpectedVariableStats(Predicate<VariableStatsEstimate> statsChecker, String sql)
    {
        assertPlanHasExpectedStats(planNodeStatsEstimate -> planNodeStatsEstimate.getVariableStatistics().values().stream().allMatch(statsChecker), sql);
    }

    @BeforeClass
    public final void init()
            throws Exception
    {
        queryRunner = createQueryRunner(ImmutableMap.of(SCALAR_FUNCTION_STATS_PROPAGATION_ENABLED, "true"));
    }

    @DataProvider(name = "queriesWithStringFunctionsInJoinClause")
    public Object[][] queriesWithStringFunctionsInJoinClause()
    {
        return ImmutableList.of(
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and reverse(trim(l.comment)) = reverse(rtrim(ltrim(l.comment)))",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and lower(l.comment) = upper(l.comment)",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and ltrim(lpad(l.comment, 10, ' ')) = rtrim(rpad(l.comment, 10, ' '))",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and substr(lower(l.comment), 2) = 'us'",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and l.comment LIKE '%u'",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and l.comment LIKE '%u%'",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and levenshtein_distance(l.comment, 'no') = 2",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and hamming_distance(l.comment, 'no') = 2",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and normalize(l.comment, NFC) = 'us'",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and from_utf8(to_utf8(l.comment)) = 'us'",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and starts_with(o.orderstatus, l.comment)",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and ends_with(o.orderstatus, l.comment)",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and concat(o.orderstatus, l.comment) LIKE '%new us%'",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and levenshtein_distance(l.comment, 'no') > 2",
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and levenshtein_distance(l.comment, 'no') < 20")
                .stream().collect(TestngUtils.toDataProvider());
    }

    @DataProvider(name = "queriesWithMathFunctionsInJoinClause")
    public Object[][] queriesWithMathFunctionsInJoinClause()
    {
        return ImmutableList.of(
                        "SELECT 1 FROM lineitem l, orders o WHERE l.orderkey=o.orderkey and l.discount = (SELECT random() FROM nation n where n.nationkey=1)",
                        "SELECT 1 FROM lineitem l, orders o WHERE l.orderkey=o.orderkey and log10(o.totalprice) > 1",
                        // "SELECT 1 FROM lineitem l, orders o WHERE l.orderkey=o.orderkey and is_nan(o.totalprice)", // failing due to source stats missing for orderkey.
                        "SELECT 1 FROM orders o, lineitem as l WHERE o.orderkey = l.orderkey and year(o.orderdate) <> year(l.shipdate) ")
                .stream().collect(TestngUtils.toDataProvider());
    }

    @Test(dataProvider = "queriesWithStringFunctionsInJoinClause")
    public void testStatsPropagationScalarStringFunction(@Language("SQL") String query)
    {
        ensurePlanNodesHaveStats(query);
    }

    @Test(dataProvider = "queriesWithMathFunctionsInJoinClause")
    public void testStatsPropagationScalarMathFunction(@Language("SQL") String query)
    {
        ensurePlanNodesHaveStats(query);
    }

    private void ensurePlanNodesHaveStats(@Language("SQL") String query)
    {
        assertPlanHasExpectedStats(planNodeStatsEstimate -> !planNodeStatsEstimate.isOutputRowCountUnknown(), query);
        assertPlanHasExpectedVariableStats(stats -> isFinite(stats.getDistinctValuesCount()), query);
        assertPlanHasExpectedVariableStats(stats -> isFinite(stats.getNullsFraction()), query);
    }
}
