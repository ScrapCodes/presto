package com.facebook.presto.spi.function;

import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.PARAMETER;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

@Retention(RUNTIME)
@Target(PARAMETER)
public @interface ScalarTypeStats
{
    boolean propagateAllStats() default true;

    PropagateSourceStats minMaxValue() default PropagateSourceStats.SOURCE_STATS;
    PropagateSourceStats distinctValueCount() default PropagateSourceStats.SOURCE_STATS;
    PropagateSourceStats avgRowSize() default PropagateSourceStats.SOURCE_STATS;
    PropagateSourceStats nullFraction() default PropagateSourceStats.SOURCE_STATS;
    PropagateSourceStats histogram() default PropagateSourceStats.SOURCE_STATS;
}
