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

import java.util.Set;

public class JoinCondition
        extends Condition
{
    private final String leftElement;
    private final String rightElement;
    private Set<String> tables;

    public JoinCondition(String leftElement, String rightElement)
    {
        this.leftElement = leftElement;
        this.rightElement = rightElement;
    }

    public JoinCondition(String leftElement, String rightElement, Set<String> tables)
    {
        this.leftElement = leftElement;
        this.rightElement = rightElement;
        this.tables = tables;
    }

    public Set<String> getTables()
    {
        return tables;
    }

    @Override
    // todo: make sure each single join condition is ordered
    public String toSQL()
    {
        if (leftElement.compareTo(rightElement) < 0) {
            return this.leftElement + " = " + this.rightElement;
        }
        else {
            return this.rightElement + " = " + this.leftElement;
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        JoinCondition that = (JoinCondition) obj;
        return (leftElement.equals(that.leftElement) && rightElement.equals(that.rightElement)) ||
                (leftElement.equals(that.rightElement) && rightElement.equals(that.leftElement));
    }

    @Override
    public int hashCode()
    {
        return leftElement.hashCode() + rightElement.hashCode();
    }
}
