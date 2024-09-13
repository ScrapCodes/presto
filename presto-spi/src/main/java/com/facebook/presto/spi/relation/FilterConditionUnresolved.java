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
package com.facebook.presto.spi.relation;

import java.util.Map;

//
public class FilterConditionUnresolved
        extends UnresolvedCondition
{
    // todo: the input should be all the filter conditions of a single table, not just a single filter
    private final RowExpression conditionExpression;
//    private final String table;
//    private final RowExpression leftArgument;
//    private final RowExpression rightArgument;
//    private final String operatorName;

    public FilterConditionUnresolved(RowExpression conditionExpression)
    {
        this.conditionExpression = conditionExpression;
//        this.table = table;
    }

    @Override
    public Condition resolveAlias(Map<VariableReferenceExpression, String> aliasToColumnMap)
    {
        return new FilterCondition(this.conditionExpression.toSQL(aliasToColumnMap));
    }
}
