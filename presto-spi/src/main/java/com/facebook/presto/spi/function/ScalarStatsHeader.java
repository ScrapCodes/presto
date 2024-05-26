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

import java.util.Map;

public class ScalarStatsHeader
{
    private Map<Integer, ScalarTypeStats> statsResolver;
    private double distinctValuesCount;
    private double nullFraction;
    private double avgRowSize;

    private ScalarStatsHeader(Map<Integer, ScalarTypeStats> statsResolver, double distinctValuesCount, double nullFraction, double avgRowSize)
    {
        this.statsResolver = statsResolver;
        this.distinctValuesCount = distinctValuesCount;
        this.nullFraction = nullFraction;
        this.avgRowSize = avgRowSize;
    }

    public ScalarStatsHeader(ScalarFunctionStats statsHeader, Map<Integer, ScalarTypeStats> statsResolver)
    {
        this(statsResolver, statsHeader.distinctValuesCount(), statsHeader.nullFraction(), statsHeader.avgRowSize());
    }

    public double getAvgRowSize()
    {
        return avgRowSize;
    }

    public double getNullFraction()
    {
        return nullFraction;
    }

    public double getDistinctValuesCount()
    {
        return distinctValuesCount;
    }

    public Map<Integer, ScalarTypeStats> getStatsResolver()
    {
        return statsResolver;
    }
}
