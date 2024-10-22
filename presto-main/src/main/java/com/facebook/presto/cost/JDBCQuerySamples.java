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

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import static java.lang.Double.NaN;

public class JDBCQuerySamples
{
    private final Connection connection;
    private final Statement statement;

    public JDBCQuerySamples(String jdbcUrl)
    {
        try {
            Class.forName("org.duckdb.DuckDBDriver");
            connection = DriverManager.getConnection(jdbcUrl);
            statement = connection.createStatement();
        }
        catch (Exception e) {
            e.printStackTrace();
            throw new PrestoException(StandardErrorCode.NOT_FOUND, String.format("Error while creating connection : %s", jdbcUrl));
        }
    }

    public double estimatedRowCounts(String query)
    {
        try {
            ResultSet resultSet = statement.executeQuery(query);
            resultSet.afterLast();
            return resultSet.getRow();
        }
        catch (SQLException e) {
            System.out.println("Error while executing query: " + query + " error: " + e.getMessage());
        }
        return NaN;
    }
}
