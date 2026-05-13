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
package com.facebook.presto.iceberg;

import com.facebook.airlift.http.server.testing.TestingHttpServer;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.WarningCollector;
import com.facebook.presto.spi.plan.FilterNode;
import com.facebook.presto.spi.relation.DeterminismEvaluator;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.PredicateCompiler;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionService;
import com.facebook.presto.sql.gen.RowExpressionPredicateCompiler;
import com.facebook.presto.sql.planner.optimizations.PlanNodeSearcher;
import com.facebook.presto.sql.planner.planPrinter.RowExpressionFormatter;
import com.facebook.presto.sql.relational.RowExpressionDeterminismEvaluator;
import com.facebook.presto.sql.relational.RowExpressionDomainTranslator;
import com.facebook.presto.sql.relational.RowExpressionOptimizer;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.testing.QueryRunner.MaterializedResultWithPlan;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import com.google.common.collect.ImmutableMap;
import org.assertj.core.util.Files;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.io.File;
import java.util.Optional;

import static com.facebook.presto.iceberg.CatalogType.REST;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.getRestServer;
import static com.facebook.presto.iceberg.rest.IcebergRestTestUtil.restConnectorProperties;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.anyTree;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.filter;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.tableScan;
import static com.google.common.io.MoreFiles.deleteRecursively;
import static com.google.common.io.RecursiveDeleteOption.ALLOW_INSECURE;
import static org.testng.Assert.assertEquals;

@Test(singleThreaded = true)
public class TestIcebergDerivedColumnOptimizer
        extends AbstractTestQueryFramework
{
    @Language("SQL") private static final String CREATE_TABLE_SQL = "CREATE TABLE test_table1 (\n" +
            " \"c1\" bigint,\n" +
            " \"c2\" varchar,\n" +
            " \"c3\" double,\n" +
            " \"c2_derived\" varchar\n" +
            " )\n" +
            "  WITH (\n" +
            "        \"derived-columns\" = Array['c2_derived'],\n" +
            "        \"derived-columns.spec.udf.json\" = JSON '{\n" +
            "        \"udfSpecList\" : [ {\n" +
            "           \"catalog\" : \"presto\",\n" +
            "            \"schema\" : \"default\",\n" +
            "           \"functionName\" : \"lower\",\n" +
            "           \"params\" : [ \"varchar\" ],\n" +
            "           \"arguments\" : [ {\n" +
            "               \"argumentIndex\" : 0,\n" +
            "               \"argumentType\" : \"varchar\",\n" +
            "               \"argumentValue\" : \"c2\",\n" +
            "               \"columnRef\" : \"COLUMN\"\n" +
            "               } ],\n" +
            "           \"derivedColumnName\" : \"c2_derived\",\n" +
            "           \"returnType\" : \"varchar\"\n" +
            "           } ]\n" +
            "       }')\n";
    private File warehouseLocation;
    private TestingHttpServer restServer;
    private static final MetadataManager METADATA = MetadataManager.createTestMetadataManager();

    private static final RowExpressionService ROW_EXPRESSION_SERVICE = new RowExpressionService()
    {
        @Override
        public DomainTranslator getDomainTranslator()
        {
            return new RowExpressionDomainTranslator(METADATA);
        }

        @Override
        public ExpressionOptimizer getExpressionOptimizer(ConnectorSession session)
        {
            return new RowExpressionOptimizer(METADATA);
        }

        @Override
        public PredicateCompiler getPredicateCompiler()
        {
            return new RowExpressionPredicateCompiler(METADATA);
        }

        @Override
        public DeterminismEvaluator getDeterminismEvaluator()
        {
            return new RowExpressionDeterminismEvaluator(METADATA);
        }

        @Override
        public String formatRowExpression(ConnectorSession session, RowExpression expression)
        {
            return new RowExpressionFormatter(METADATA.getFunctionAndTypeManager()).formatRowExpression(session, expression);
        }
    };

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        warehouseLocation = Files.newTemporaryFolder();
        restServer = getRestServer(warehouseLocation.getAbsolutePath());
        restServer.start();
        super.init();
    }

    @AfterClass(alwaysRun = true)
    public void tearDown()
            throws Exception
    {
        if (restServer != null) {
            restServer.stop();
        }
        if (warehouseLocation != null) {
            deleteRecursively(warehouseLocation.toPath(), ALLOW_INSECURE);
        }
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return IcebergQueryRunner.builder()
                .setCatalogType(REST)
                .setExtraConnectorProperties(ImmutableMap.<String, String>builder()
                        .putAll(restConnectorProperties(restServer.getBaseUrl().toString()))
                        .put("iceberg.derived_columns.enable", "true")
                        .build())
                .setDataDirectory(Optional.of(warehouseLocation.toPath()))
                .setSchemaName("test_schema")
                .setCreateTpchTables(false)
                .setAddJmxPlugin(false)
                .build()
                .getQueryRunner();
    }

    @Test
    public void testBasicFilterPredicateRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            assertQuery("SELECT * FROM test_table1 WHERE lower(c2) = 'a'", "VALUES (121, 'A', 12.1, 'a')");
            assertQuery("SELECT * FROM test_table1 WHERE upper(c2) = 'A'", "VALUES (121, 'A', 12.1, 'a')");
            assertPlan("SELECT * FROM test_table1 WHERE upper(c2) = 'A'",
                    anyTree(filter("(upper(c2)) = (VARCHAR'A')", tableScan("test_table1", ImmutableMap.of("c1", "c1", "c2", "c2")))));
            assertPlan("SELECT * FROM test_table1 WHERE lower(c2) = 'a'",
                    anyTree(filter("(c2_derived) = (VARCHAR'a')", tableScan("test_table1",
                            ImmutableMap.of("c1", "c1", "c2", "c2", "c2_derived", "c2_derived")))));
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    @Test
    public void testSelectWithDerivedColumnNotProjectedFilterPredicateRewrite()
    {
        try {
            assertUpdate(CREATE_TABLE_SQL);
            assertUpdate("INSERT INTO test_table1 VALUES (123, 'B', 12.2, lower('B')), (120, 'C', 12.3, lower('C')), (121, 'A', 12.1, lower('A'))", 3);
            // The following query does not project derived column i.e. c2_derived.
            @Language("SQL") String query = "SELECT c1 FROM test_table1 WHERE lower(c2) = 'a'";
            assertQuery(query, "VALUES 121");
            assertQuery("SELECT c1 FROM test_table1 WHERE lower(c2) = 'a' AND c1 = 121", "VALUES 121");
            // TODO: Pattern based plan matchers failed to work.
            MaterializedResultWithPlan resultWithPlan = getQueryRunner().executeWithPlan(getSession(), query, WarningCollector.NOOP);
            FilterNode filter = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof FilterNode).findOnlyElement();
            String formattedRowExpression = ROW_EXPRESSION_SERVICE.formatRowExpression(getSession().toConnectorSession(), filter.getPredicate());
            assertEquals(formattedRowExpression, "(c2_derived) = (VARCHAR'a')");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table1");
        }
    }

    @Test
    public void testUdfSpecWithMoreThanOneUDFAndMultiArgUDFsSpecified()
    {
        try {
            assertUpdate(" CREATE TABLE test_table2 (                   \n" +
                    "     \"c1\" bigint,                                                 \n" +
                    "     \"c2\" varchar,                                                \n" +
                    "     \"c3\" double,\n" +
                    "     \"c2_derived\" varchar,\n" +
                    "     \"c2_derived2\" varchar\n" +
                    "  )                                                               \n" +
                    "  WITH (                                                          \n" +
                    "     \"derived-columns\" = Array['c2_derived', 'c2_derived2'],\n" +
                    "     \"derived-columns.spec.udf.json\" = JSON '{\n" +
                    "      \"udfSpecList\": [{\n" +
                    "                \"catalog\": \"presto\",\n" +
                    "                \"schema\": \"default\",\n" +
                    "                \"functionName\": \"lower\",\n" +
                    "                \"params\": [\"varchar\"],\n" +
                    "                \"arguments\": [{\n" +
                    "                     \"argumentIndex\": 0,\n" +
                    "                     \"argumentType\": \"varchar\",\n" +
                    "                     \"argumentValue\": \"c2\",\n" +
                    "                     \"columnRef\": \"COLUMN\"\n" +
                    "                }],\n" +
                    "                \"derivedColumnName\": \"c2_derived\",\n" +
                    "                \"returnType\": \"varchar\"\n" +
                    "           },\n" +
                    "           {\n" +
                    "               \"catalog\": \"presto\",\n" +
                    "                \"schema\": \"default\",\n" +
                    "                \"functionName\": \"lpad\",\n" +
                    "                \"params\": [\"varchar\", \"bigint\", \"varchar\"],\n" +
                    "                \"arguments\": [{\n" +
                    "                     \"argumentIndex\": 0,\n" +
                    "                     \"argumentType\": \"varchar\",\n" +
                    "                     \"argumentValue\": \"c2\",\n" +
                    "                   \"columnRef\": \"COLUMN\"\n" +
                    "                }, {\n" +
                    "                     \"argumentIndex\": 1,\n" +
                    "                     \"argumentType\": \"bigint\",\n" +
                    "                     \"argumentValue\": \"10\",\n" +
                    "                     \"columnRef\": \"CONSTANT\"\n" +
                    "                }, {\n" +
                    "                     \"argumentIndex\": 2,\n" +
                    "                     \"argumentType\": \"varchar\",\n" +
                    "                     \"argumentValue\": \"X\",\n" +
                    "                     \"columnRef\": \"CONSTANT\"\n" +
                    "                }],\n" +
                    "              \"derivedColumnName\": \"c2_derived2\",\n" +
                    "                \"returnType\": \"varchar\"\n" +
                    "           }\n" +
                    "      ]\n" +
                    " }')\n");
            assertUpdate("INSERT INTO test_table2 VALUES (123, 'B', 12.2, lower('B'), lpad('B', 10, 'X')), (120, 'C', 12.3, lower('C'), lpad('C', 10, 'X'))," +
                    " (121, 'A', 12.1, lower('A'), lpad('A', 10, 'X'))", 3);
            @Language("SQL") String query = "SELECT c1, c2 FROM test_table2 WHERE c1 = 100 OR (lower(c2) = 'a' AND lpad(c2, 10, 'X') = 'XXXXXXXXXA' ) OR c2 LIKE '%Z%'";
            assertQuery(query, "VALUES (121, 'A')");
            assertQuery("SELECT c1 FROM test_table2 WHERE lower(c2) = 'a' AND c1 = 121", "VALUES 121");
            MaterializedResultWithPlan resultWithPlan = getQueryRunner().executeWithPlan(getSession(), query, WarningCollector.NOOP);
            FilterNode filter = PlanNodeSearcher.searchFrom(resultWithPlan.getQueryPlan().getRoot()).where(planNode -> planNode instanceof FilterNode).findOnlyElement();
            String formattedRowExpression = ROW_EXPRESSION_SERVICE.formatRowExpression(getSession().toConnectorSession(), filter.getPredicate());
            assertEquals(formattedRowExpression, "((((c1) = (BIGINT'100')) OR ((STRPOS(c2, VARCHAR'Z')) <> (BIGINT'0'))) OR ((c2_derived) = (VARCHAR'a'))) AND " +
                    "((((c1) = (BIGINT'100')) OR ((STRPOS(c2, VARCHAR'Z')) <> (BIGINT'0'))) OR ((c2_derived2) = (VARCHAR'XXXXXXXXXA')))");
        }
        finally {
            assertUpdate("DROP TABLE IF EXISTS test_table2");
        }
    }
}
