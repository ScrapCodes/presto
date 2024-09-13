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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.function.FunctionHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.concurrent.Immutable;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import static com.facebook.presto.common.function.OperatorType.EQUAL;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN;
import static com.facebook.presto.common.function.OperatorType.GREATER_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN;
import static com.facebook.presto.common.function.OperatorType.LESS_THAN_OR_EQUAL;
import static com.facebook.presto.common.function.OperatorType.NOT_EQUAL;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

@Immutable
public final class CallExpression
        extends RowExpression
{
    private final String displayName;
    private final FunctionHandle functionHandle;
    private final Type returnType;
    private final List<RowExpression> arguments;

    public CallExpression(
            // The name here should only be used for display (toString)
            String displayName,
            FunctionHandle functionHandle,
            Type returnType,
            List<RowExpression> arguments)
    {
        this(arguments.stream()
                .filter(x -> x.getSourceLocation().isPresent())
                .map(x -> x.getSourceLocation())
                .findFirst().orElse(Optional.empty()),
                displayName, functionHandle, returnType, arguments);
    }

    @JsonCreator
    public CallExpression(
            @JsonProperty("sourceLocation") Optional<SourceLocation> sourceLocation,
            // The name here should only be used for display (toString)
            @JsonProperty("displayName") String displayName,
            @JsonProperty("functionHandle") FunctionHandle functionHandle,
            @JsonProperty("returnType") Type returnType,
            @JsonProperty("arguments") List<RowExpression> arguments)
    {
        super(sourceLocation);
        this.displayName = requireNonNull(displayName, "displayName is null");
        this.functionHandle = requireNonNull(functionHandle, "functionHandle is null");
        this.returnType = requireNonNull(returnType, "returnType is null");
        this.arguments = unmodifiableList(new ArrayList<>(requireNonNull(arguments, "arguments is null")));
    }

    @JsonProperty
    public String getDisplayName()
    {
        return displayName;
    }

    @JsonProperty
    public FunctionHandle getFunctionHandle()
    {
        return functionHandle;
    }

    @Override
    @JsonProperty("returnType")
    public Type getType()
    {
        return returnType;
    }

    @Override
    public List<RowExpression> getChildren()
    {
        return arguments;
    }

    @JsonProperty
    public List<RowExpression> getArguments()
    {
        return arguments;
    }

    @Override
    public String toString()
    {
        return displayName + "(" + String.join(", ", arguments.stream().map(RowExpression::toString).collect(Collectors.toList())) + ")";
    }

    @Override
    public String toSQL(Map<VariableReferenceExpression, String> aliasToColumnMap)
    {
        // TODO: the first argument is rowexpression, not column name
        // TODO: for now, assume it is a binary comparison
        String ope = null;
        assert arguments.size() == 2;
        // TODO: add more operator type
        if (displayName.equals(EQUAL.name())) {
            ope = EQUAL.getOperator();
        }
        else if (displayName.equals(GREATER_THAN_OR_EQUAL.name())) {
            ope = GREATER_THAN_OR_EQUAL.getOperator();
        }
        else if (displayName.equals(GREATER_THAN.name())) {
            ope = GREATER_THAN.getOperator();
        }
        else if (displayName.equals(LESS_THAN_OR_EQUAL.name())) {
            ope = LESS_THAN_OR_EQUAL.getOperator();
        }
        else if (displayName.equals(LESS_THAN.name())) {
            ope = LESS_THAN.getOperator();
        }
        else if (displayName.equals(NOT_EQUAL.name())) {
            ope = NOT_EQUAL.getOperator();
        }
        if (ope != null) {
            String leftOperand = null;
            String rightOperand = null;
            RowExpression leftExpression = arguments.get(0);
            RowExpression rightExpression = arguments.get(1);
            if (leftExpression instanceof VariableReferenceExpression && aliasToColumnMap.containsKey(leftExpression)) {
                leftOperand = aliasToColumnMap.get(leftExpression);
            }
            if (leftExpression instanceof ConstantExpression) {
                leftOperand = leftExpression.toString();
            }
            if (rightExpression instanceof VariableReferenceExpression && aliasToColumnMap.containsKey(rightExpression)) {
                rightOperand = aliasToColumnMap.get(rightExpression);
            }
            if (rightExpression instanceof ConstantExpression) {
                rightOperand = rightExpression.toString();
            }
            if (leftExpression != null && rightExpression != null) {
                return leftOperand + ope + rightOperand;
            }
        }
        return null;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(functionHandle, arguments);
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
        CallExpression other = (CallExpression) obj;
        return Objects.equals(this.functionHandle, other.functionHandle) && Objects.equals(this.arguments, other.arguments);
    }

    @Override
    public <R, C> R accept(RowExpressionVisitor<R, C> visitor, C context)
    {
        return visitor.visitCall(this, context);
    }

    @Override
    public RowExpression canonicalize()
    {
        return getSourceLocation().isPresent() ? new CallExpression(Optional.empty(), displayName, functionHandle, returnType, arguments) : this;
    }
}
