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

public class ScalarStatsHeader
{
    private boolean propagateStats;
    private boolean isIntersection;
    private double distinctValuesCount;
    private double distinctValCountAdjustFactor;
    private double nullFraction;
    private double nullFractionAdjustFactor;
    private double avgRowSize;
    private double avgRowSizeAdjustFactor;

    private ScalarStatsHeader(boolean propagateStats, boolean isIntersection, double distinctValuesCount, double distinctValCountAdjustFactor, double nullFraction,
            double nullFractionAdjustFactor, double avgRowSize, double avgRowSizeAdjustFactor)
    {
        this.propagateStats = propagateStats;
        this.isIntersection = isIntersection;
        this.distinctValuesCount = distinctValuesCount;
        this.distinctValCountAdjustFactor = distinctValCountAdjustFactor;
        this.nullFraction = nullFraction;
        this.nullFractionAdjustFactor = nullFractionAdjustFactor;
        this.avgRowSize = avgRowSize;
        this.avgRowSizeAdjustFactor = avgRowSizeAdjustFactor;
    }

    public ScalarStatsHeader(ScalarFunctionStatsCalculator statsHeader)
    {
        this(statsHeader.propagateStats(), statsHeader.isIntersection(), statsHeader.distinctValuesCount(), statsHeader.distinctValCountAdjustFactor(),
                statsHeader.nullFraction(), statsHeader.nullFractionAdjustFactor(), statsHeader.avgRowSize(), statsHeader.avgRowSizeAdjustFactor());
    }

    public double getAvgRowSizeAdjustFactor()
    {
        return avgRowSizeAdjustFactor;
    }

    public double getAvgRowSize()
    {
        return avgRowSize;
    }

    public double getNullFractionAdjustFactor()
    {
        return nullFractionAdjustFactor;
    }

    public double getNullFraction()
    {
        return nullFraction;
    }

    public double getDistinctValCountAdjustFactor()
    {
        return distinctValCountAdjustFactor;
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public boolean isIntersection()
    {
        return isIntersection;
    }

    public boolean isPropagateStats()
    {
        return propagateStats;
    }
}
