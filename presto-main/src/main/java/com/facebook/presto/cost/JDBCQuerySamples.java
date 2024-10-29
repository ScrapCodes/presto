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

import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.google.common.collect.ImmutableList;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Collections;
import java.util.List;
import java.util.Set;

public class JDBCQuerySamples
{
    private final Connection connection;

    public JDBCQuerySamples(String jdbcUrl)
    {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            connection = DriverManager.getConnection(jdbcUrl);
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new PrestoException(StandardErrorCode.NOT_FOUND, String.format("Error while creating connection : %s", jdbcUrl));
        }
    }

    public List<Double> estimatedRowCounts(Set<String> tables, String query)
    {
        if (tables != null) {
            try (Statement statement = connection.createStatement()) {
                String sql = "SELECT COUNT(*) FROM " + String.join(",", tables) + " WHERE " + query;
                ResultSet resultSet = statement.executeQuery(sql);
                System.out.println("Executing query: " + sql);
                ImmutableList.Builder<Double> listBuilder = ImmutableList.builder();
                if (resultSet.next()) {
                    listBuilder.add(resultSet.getDouble(1));
                }
                return listBuilder.build();
            }
            catch (SQLException e) {
                System.out.println("Error while executing query: " + query + " error: " + e.getMessage());
            }
        }
        return Collections.emptyList();
    }
}
