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

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class JoinConditionUnresolved
        extends UnresolvedCondition
{
    // for JoinNode, extract each Join condition
    private final VariableReferenceExpression leftPredicate;
    private final VariableReferenceExpression rightPredicate;
    private final Set<String> tableNames = new HashSet<>();

    public JoinConditionUnresolved(VariableReferenceExpression leftPredicate, VariableReferenceExpression rightPredicate)
    {
        this.leftPredicate = leftPredicate;
        this.rightPredicate = rightPredicate;
    }

    public VariableReferenceExpression getLeftPredicate()
    {
        return leftPredicate;
    }

    public VariableReferenceExpression getRightPredicate()
    {
        return rightPredicate;
    }

    @Override
    public Condition resolveAlias(Map<VariableReferenceExpression, Map<String, String>> aliasToColumnMap)
    {
        if (aliasToColumnMap.containsKey(leftPredicate) && aliasToColumnMap.containsKey(rightPredicate)) {
            String tableFromLeftPredicate = aliasToColumnMap.get(leftPredicate).keySet().stream().findFirst().get();
            String tableFromRightPredicate = aliasToColumnMap.get(rightPredicate).keySet().stream().findFirst().get();
            return new JoinCondition(aliasToColumnMap.get(leftPredicate).values().stream().findFirst().get(),
                    aliasToColumnMap.get(rightPredicate).values().stream().findFirst().get(),
                    Collections.unmodifiableSet(new HashSet<>(Arrays.asList(tableFromLeftPredicate, tableFromRightPredicate))));
        }
        return null;
    }
}
