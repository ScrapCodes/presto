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

package com.facebook.presto.sql.planner;

import com.facebook.presto.Session;
import com.facebook.presto.spi.ConnectorTableHandle;
import com.facebook.presto.spi.plan.AggregationNode;
import com.facebook.presto.spi.plan.JoinDistributionType;
import com.facebook.presto.spi.plan.Partitioning;
import com.facebook.presto.spi.plan.TableScanNode;
import com.facebook.presto.spi.plan.ValuesNode;
import com.facebook.presto.sql.planner.assertions.BasePlanTest;
import com.facebook.presto.sql.planner.plan.ExchangeNode;
import com.facebook.presto.sql.planner.plan.JoinNode;
import com.facebook.presto.sql.planner.plan.SemiJoinNode;
import com.facebook.presto.tpcds.TpcdsTableHandle;
import com.facebook.presto.tpch.TpchTableHandle;
import com.google.common.base.Strings;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Resources;
import org.testng.annotations.DataProvider;
import org.testng.annotations.Test;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Stream;

import static com.facebook.presto.SystemSessionProperties.OPTIMIZER_USE_HISTOGRAMS;
import static com.facebook.presto.SystemSessionProperties.SCALAR_FUNCTION_STATS_PROPAGATION_ENABLED;
import static com.facebook.presto.spi.plan.JoinDistributionType.REPLICATED;
import static com.facebook.presto.spi.plan.JoinType.INNER;
import static com.facebook.presto.sql.Optimizer.PlanStage.OPTIMIZED_AND_VALIDATED;
import static com.facebook.presto.testing.TestngUtils.toDataProviderFromArray;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Verify.verify;
import static com.google.common.io.Files.createParentDirs;
import static com.google.common.io.Files.write;
import static com.google.common.io.Resources.getResource;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.nio.file.Files.isDirectory;
import static java.util.Locale.ENGLISH;
import static java.util.stream.Collectors.joining;
import static org.testng.Assert.assertEquals;

public abstract class AbstractCostBasedPlanTest
        extends BasePlanTest
{
    private static final String NO_FEATURE_ENABLED = "no_feature_enabled";

    private final Map<String, String> featuresMap =
            ImmutableMap.of(
                    NO_FEATURE_ENABLED, "",
                    "histogram", OPTIMIZER_USE_HISTOGRAMS,
                    "scalar_function_stats_propagation", SCALAR_FUNCTION_STATS_PROPAGATION_ENABLED);

    public AbstractCostBasedPlanTest(LocalQueryRunnerSupplier supplier)
    {
        super(supplier);
    }

    protected abstract Stream<String> getQueryResourcePaths();

    @DataProvider
    public Object[][] getQueriesDataProvider()
    {
        return featuresMap.keySet().stream().flatMap(feature -> getQueryResourcePaths().map(path -> new String[] {feature, path})).collect(toDataProviderFromArray());
    }

    @Test(dataProvider = "getQueriesDataProvider")
    public void test(String feature, String queryResourcePath)
    {
        String sql = read(queryResourcePath);
        if (!feature.equals(NO_FEATURE_ENABLED)) {
            Session featureEnabledSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(featuresMap.get(feature), "true")
                    .build();
            Session featureDisabledSession = Session.builder(getQueryRunner().getDefaultSession())
                    .setSystemProperty(featuresMap.get(feature), "false")
                    .build();
            String regularPlan = generateQueryPlan(sql, featureDisabledSession);
            String featureEnabledPlan = generateQueryPlan(sql, featureEnabledSession);
            if (!regularPlan.equals(featureEnabledPlan)) {
                assertEquals(featureEnabledPlan, read(getSpecificPlanResourcePath(feature, getQueryPlanResourcePath(queryResourcePath))));
            }
        }
        else {
            assertEquals(generateQueryPlan(sql), read(getQueryPlanResourcePath(queryResourcePath)));
        }
    }

    private String getQueryPlanResourcePath(String queryResourcePath)
    {
        return queryResourcePath.replaceAll("\\.sql$", ".plan.txt");
    }

    private String getSpecificPlanResourcePath(String outDirPath, String regularPlanResourcePath)
    {
        Path root = Paths.get(regularPlanResourcePath);
        return root.getParent().resolve(format("%s/%s", outDirPath, root.getFileName())).toString();
    }

    private Path getResourceWritePath(String queryResourcePath)
    {
        return Paths.get(
                getSourcePath().toString(),
                "src/test/resources",
                getQueryPlanResourcePath(queryResourcePath));
    }

    public void generate()
            throws Exception
    {
        initPlanTest();
        try {
            getQueryResourcePaths()
                    .parallel()
                    .forEach(queryResourcePath -> {
                        try {
                            for (Entry<String, String> featureEntry : featuresMap.entrySet()) {
                                String sql = read(queryResourcePath);
                                if (!featureEntry.getKey().equals(NO_FEATURE_ENABLED)) {
                                    Session featureDisabledSession = Session.builder(getQueryRunner().getDefaultSession())
                                            .setSystemProperty(featureEntry.getValue(), "false")
                                            .build();
                                    String regularPlan = generateQueryPlan(sql, featureDisabledSession);
                                    Session featureEnabledSession = Session.builder(getQueryRunner().getDefaultSession())
                                            .setSystemProperty(featureEntry.getValue(), "true")
                                            .build();
                                    String featureEnabledPlan = generateQueryPlan(sql, featureEnabledSession);
                                    // write out the feature enabled plan if it differs
                                    if (!regularPlan.equals(featureEnabledPlan)) {
                                        Path featureEnabledPlanWritePath = getResourceWritePath(getSpecificPlanResourcePath(featureEntry.getKey(), queryResourcePath));
                                        createParentDirs(featureEnabledPlanWritePath.toFile());
                                        write(featureEnabledPlan.getBytes(UTF_8), featureEnabledPlanWritePath.toFile());
                                    }
                                }
                                else {
                                    Path queryPlanWritePath = getResourceWritePath(queryResourcePath);
                                    createParentDirs(queryPlanWritePath.toFile());
                                    write(generateQueryPlan(sql).getBytes(UTF_8), queryPlanWritePath.toFile());
                                }
                                System.out.println("Generated expected plan for query: " + queryResourcePath);
                            }
                        }
                        catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    });
        }
        finally {
            destroyPlanTest();
        }
    }

    private static String read(String resource)
    {
        try {
            return Resources.toString(getResource(AbstractCostBasedPlanTest.class, resource), UTF_8);
        }
        catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private String generateQueryPlan(String query)
    {
        return generateQueryPlan(query, getQueryRunner().getDefaultSession());
    }

    private String generateQueryPlan(String query, Session session)
    {
        String sql = query.replaceAll("\\s+;\\s+$", "")
                .replace("${database}.${schema}.", "")
                .replace("\"${database}\".\"${schema}\".\"${prefix}", "\"");
        Plan plan = plan(session, sql, OPTIMIZED_AND_VALIDATED, false);

        JoinOrderPrinter joinOrderPrinter = new JoinOrderPrinter();
        plan.getRoot().accept(joinOrderPrinter, 0);
        return joinOrderPrinter.result();
    }

    private static Path getSourcePath()
    {
        Path workingDir = Paths.get(System.getProperty("user.dir"));
        verify(isDirectory(workingDir), "Working directory is not a directory");
        String topDirectoryName = workingDir.getFileName().toString();
        switch (topDirectoryName) {
            case "presto-benchto-benchmarks":
                return workingDir;
            case "presto":
                return workingDir.resolve("presto-benchto-benchmarks");
            default:
                throw new IllegalStateException("This class must be executed from presto-benchto-benchmarks or presto source directory");
        }
    }

    private static class JoinOrderPrinter
            extends SimplePlanVisitor<Integer>
    {
        private final StringBuilder result = new StringBuilder();

        public String result()
        {
            return result.toString();
        }

        @Override
        public Void visitJoin(JoinNode node, Integer indent)
        {
            JoinDistributionType distributionType = node.getDistributionType()
                    .orElseThrow(() -> new VerifyException("Expected distribution type to be set"));
            if (node.isCrossJoin()) {
                checkState(node.getType() == INNER && distributionType == REPLICATED, "Expected CROSS JOIN to be INNER REPLICATED");
                output(indent, "cross join:");
            }
            else {
                output(indent, "join (%s, %s):", node.getType(), distributionType);
            }

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitExchange(ExchangeNode node, Integer indent)
        {
            Partitioning partitioning = node.getPartitioningScheme().getPartitioning();
            output(
                    indent,
                    "%s exchange (%s, %s, %s)",
                    node.getScope().isRemote() ? "remote" : "local",
                    node.getType(),
                    partitioning.getHandle(),
                    partitioning.getArguments().stream()
                            .map(Object::toString)
                            .sorted() // Currently, order of hash columns is not deterministic
                            .collect(joining(", ", "[", "]")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitAggregation(AggregationNode node, Integer indent)
        {
            output(
                    indent,
                    "%s aggregation over (%s)",
                    node.getStep().name().toLowerCase(ENGLISH),
                    node.getGroupingKeys().stream()
                            .map(Object::toString)
                            .sorted()
                            .collect(joining(", ")));

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitTableScan(TableScanNode node, Integer indent)
        {
            ConnectorTableHandle connectorTableHandle = node.getTable().getConnectorHandle();
            if (connectorTableHandle instanceof TpcdsTableHandle) {
                output(indent, "scan %s", ((TpcdsTableHandle) connectorTableHandle).getTableName());
            }
            else if (connectorTableHandle instanceof TpchTableHandle) {
                output(indent, "scan %s", ((TpchTableHandle) connectorTableHandle).getTableName());
            }
            else {
                throw new IllegalStateException(format("Unexpected ConnectorTableHandle: %s", connectorTableHandle.getClass()));
            }

            return null;
        }

        @Override
        public Void visitSemiJoin(final SemiJoinNode node, Integer indent)
        {
            output(indent, "semijoin (%s):", node.getDistributionType().get());

            return visitPlan(node, indent + 1);
        }

        @Override
        public Void visitValues(ValuesNode node, Integer indent)
        {
            output(indent, "values (%s rows)", node.getRows().size());

            return null;
        }

        private void output(int indent, String message, Object... args)
        {
            String formattedMessage = format(message, args);
            result.append(format("%s%s%n", Strings.repeat("    ", indent), formattedMessage));
        }
    }
}
