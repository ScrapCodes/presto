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

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.ElementType.TYPE;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * By default, a function is just a “black box” that the database system knows very little about the behavior of.
 * However, that means that queries using the function may be executed much less efficiently than they could be.
 * It is possible to supply additional knowledge that helps the planner optimize function calls.
 * Scalar functions are straight forward to optimize and can have impact on the overall query performance.
 * Use this annotation to provide information regarding how this function impacts following query statistics.
 * <p>
 * A function may take one or more input column or a constant as parameters. Precise stats may depend on the input
 * parameters. This annotation does not cover all the possible cases.
 * </p>
 * <ul>
 * <li>
 * 1. Provide multipliers for computing `nullFraction` and `avgRowSize`. These multiplier will be
 * applied to input columns on which this function operates on. 1.0 is the default value indicating
 * Propagate input columns stats as is.
 * </li>
 * <li>
 * 2. How to estimate number of distinct values. Does this function produces an intersection of
 * Distinct value counts of each `input column param` or a union.
 * </li>
 * </ul>
 */
@Retention(RUNTIME)
@Target({METHOD, TYPE})
public @interface ScalarFunctionStatsCalculator
{
    /**
     * Disable or enable propagate stats behaviour. e.g. upper(Slice) will not alter any source stats for the
     * input column passed as an argument. In this case it might be possible to just set propagate stats as true.
     * In cases where number of input columns are more than one, one has few choices i.e. is the resulting statistics
     * a union of both column or an intersection. To achieve this behaviour please set `isIntersection` field
     * in conjunction of setting this. For more precise control consider supplying a callback via ... see Phase 3.
     */
    boolean propagateStats() default false;

    boolean isIntersection() default false;

    /**
     * Distinct values count is another important statistic in measuring query perf characteristics,
     * Does this function produces a constant Distinct value count regardless of `input column`'s source stats.
     */
    double distinctValuesCount() default Double.NaN;

    /**
     * distinctValuesCountMultiplier: a fraction multiplied to input column's distinctValuesCount.
     */
    double distinctValuesCountMultiplier() default 1.0;

    /**
     * Does this function produce a constant nullFraction, e.g. is_null(Slice) will alter column's null fraction
     * value to 0.0.
     */
    double nullFraction() default Double.NaN;

    /**
     * A `nullFraction` Fraction of column's entries that are null and nullFractionMultiplier is the
     * number that is multiplied to input column's nullFraction value to compute this functions impact
     * on overall `nullFraction`.
     */
    double nullFractionMultiplier() default 1.0;

    /**
     * An `avgRowSize`: does this function impacts the size of each row e.g. a function like md5 may produce a
     * constant row size.
     */
    double avgRowSize() default Double.NaN;

    /**
     * An `avgRowSizeMultiplier`: does this function impacts the size of each row e.g. a function may alter the
     * column type to a wider type e.g. varchar(20) -> varchar(40).
     */
    double avgRowSizeMultiplier() default 1.0;
}
