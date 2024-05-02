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
    private double distinctValuesCountMultiplier;
    private double nullFraction;
    private double nullFractionMultiplier;
    private double avgRowSize;
    private double avgRowSizeMultiplier;

    private ScalarStatsHeader(boolean propagateStats, boolean isIntersection, double distinctValuesCount, double distinctValuesCountMultiplier, double nullFraction,
            double nullFractionMultiplier, double avgRowSize, double avgRowSizeMultiplier)
    {
        this.propagateStats = propagateStats;
        this.isIntersection = isIntersection;
        this.distinctValuesCount = distinctValuesCount;
        this.distinctValuesCountMultiplier = distinctValuesCountMultiplier;
        this.nullFraction = nullFraction;
        this.nullFractionMultiplier = nullFractionMultiplier;
        this.avgRowSize = avgRowSize;
        this.avgRowSizeMultiplier = avgRowSizeMultiplier;
    }

    public ScalarStatsHeader(ScalarFunctionStatsCalculator statsHeader)
    {
        new ScalarStatsHeader(statsHeader.propagateStats(), statsHeader.isIntersection(), statsHeader.distinctValuesCount(), statsHeader.distinctValuesCountMultiplier(),
                statsHeader.nullFraction(), statsHeader.nullFractionMultiplier(), statsHeader.avgRowSize(), statsHeader.avgRowSizeMultiplier());
    }

    public double getAvgRowSizeMultiplier()
    {
        return avgRowSizeMultiplier;
    }

    public double getAvgRowSize()
    {
        return avgRowSize;
    }

    public double getNullFractionMultiplier()
    {
        return nullFractionMultiplier;
    }

    public double getNullFraction()
    {
        return nullFraction;
    }

    public double getDistinctValuesCountMultiplier()
    {
        return distinctValuesCountMultiplier;
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
