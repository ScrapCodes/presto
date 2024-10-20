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
package com.facebook.presto.spi.function;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.unmodifiableSet;

public enum StatsPropagationBehavior
{
    /** Use the max value across all arguments to derive the new stats value */
    USE_MAX_ARGUMENT,
    /** Use the min value across all arguments to derive the new stats value */
    USE_MIN_ARGUMENT,
    /** Sum the stats value of all arguments to derive the new stats value */
    SUM_ARGUMENTS,
    /** Sum the stats value of all arguments to derive the new stats value, but upper bounded to row count. */
    SUM_ARGUMENTS_UPPER_BOUNDED_TO_ROW_COUNT,
    /** Propagate source stat if source distinct values count is less than 1% of row count */
    CONDITIONALLY_USE_SOURCE_STATS,
    /** Propagate the source stats as-is */
    USE_SOURCE_STATS,
    /** calculate logarithm with base 10 of the arguments source stats */
    LOG10_SOURCE_STATS,
    /** calculate logarithm with base 2 of the arguments source stats */
    LOG2_SOURCE_STATS,
    /** calculate natural logarithm of the arguments source stats */
    LOG_NATURAL_SOURCE_STATS,
    // Following stats are independent of source stats.
    /** Use the value of output row count. */
    ROW_COUNT,
    /** Use the value of row_count * (1 - null_fraction). */
    NON_NULL_ROW_COUNT,
    /** use the value of TYPE_WIDTH in varchar(TYPE_WIDTH) */
    USE_TYPE_WIDTH_VARCHAR,
    /** Take max of type width of arguments with varchar type. */
    MAX_TYPE_WIDTH_VARCHAR,
    /** Special case for estimating row size for substr function. */
    SUBSTR_ROW_SIZE,
    /** Stats are unknown and thus no action is performed. */
    UNKNOWN;
    /*
     * Stats are multi argument when their value is calculated by operating on stats from source stats or other properties of the all the arguments.
     */
    private static final Set<StatsPropagationBehavior> MULTI_ARGUMENT_STATS =
            unmodifiableSet(
                    new HashSet<>(Arrays.asList(MAX_TYPE_WIDTH_VARCHAR, USE_MAX_ARGUMENT, USE_MIN_ARGUMENT, SUM_ARGUMENTS, SUM_ARGUMENTS_UPPER_BOUNDED_TO_ROW_COUNT)));
    private static final Set<StatsPropagationBehavior> SOURCE_STATS_DEPENDENT_STATS =
            unmodifiableSet(
                    new HashSet<>(Arrays.asList(USE_MAX_ARGUMENT, USE_MIN_ARGUMENT, SUM_ARGUMENTS, SUM_ARGUMENTS_UPPER_BOUNDED_TO_ROW_COUNT, USE_SOURCE_STATS)));

    public static final class Constants
    {
        public static final int ROW_COUNT_CONST = -1;
        public static final int NON_NULL_ROW_COUNT_CONST = -10;
    }

    public boolean isMultiArgumentStat()
    {
        return MULTI_ARGUMENT_STATS.contains(this);
    }

    public boolean isSourceStatsDependentStats()
    {
        return SOURCE_STATS_DEPENDENT_STATS.contains(this);
    }
}
