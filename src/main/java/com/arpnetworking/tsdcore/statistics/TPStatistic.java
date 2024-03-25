/*
 * Copyright 2014 Brandon Arp
 *
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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;

import java.text.DecimalFormat;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * Base class for percentile based statistics.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class TPStatistic extends BaseStatistic implements OrderedStatistic {

    /**
     * Accessor for the percentile from 0 to 100 (inclusive).
     *
     * @return The percentile.
     */
    public double getPercentile() {
        return _percentile;
    }

    @Override
    public String getName() {
        return _defaultName;
    }

    @Override
    public Set<String> getAliases() {
        return Collections.unmodifiableSet(_aliases);
    }

    @Override
    public Calculator<NullSupportingData> createCalculator() {
        return new PercentileCalculator(this);
    }

    @Override
    public Set<Statistic> getDependencies() {
        return DEPENDENCIES.get();
    }

    @Override
    public Quantity calculate(final List<Quantity> orderedValues) {
        final int index = (int) (Math.ceil((_percentile / 100.0) * (orderedValues.size() - 1)));
        return orderedValues.get(index);
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        final List<Quantity> allSamples = Lists.newArrayList();
        for (final AggregatedData aggregation : aggregations) {
            allSamples.addAll(aggregation.getSamples());
        }
        Collections.sort(allSamples);
        final int index = (int) (Math.ceil((_percentile / 100.0) * (allSamples.size() - 1)));
        return allSamples.get(index);
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof TPStatistic)) {
            return false;
        }
        final TPStatistic otherTPStatistic = (TPStatistic) o;
        return getName().equals(otherTPStatistic.getName());
    }

    /**
     * Public constructor.
     *
     * @param percentile the percentile value to compute
     */
    public TPStatistic(final double percentile) {
        _percentile = percentile;
        _defaultName = "tp" + FORMAT.format(_percentile);
        _aliases = Sets.newHashSet();
        _aliases.add(_defaultName);
        _aliases.add(_defaultName.substring(1));
        _aliases.add(_defaultName.replace(".", "p"));
        _aliases.add(_defaultName.substring(1).replace(".", "p"));
    }

    private final double _percentile;
    private final String _defaultName;
    private final HashSet<String> _aliases;

    private static final DecimalFormat FORMAT = new DecimalFormat("##0.#");
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Supplier<Statistic> HISTOGRAM_STATISTIC =
            Suppliers.memoize(() -> STATISTIC_FACTORY.getStatistic("histogram"));
    private static final Supplier<Set<Statistic>> DEPENDENCIES =
            Suppliers.memoize(() -> ImmutableSet.of(HISTOGRAM_STATISTIC.get()));
    private static final long serialVersionUID = 2002333257077042351L;

    /**
     * Calculator computing the percentile of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class PercentileCalculator extends BaseCalculator<NullSupportingData> {

        /**
         * Public constructor.
         *
         * @param statistic The {@link TPStatistic}.
         */
        public PercentileCalculator(final TPStatistic statistic) {
            super(statistic);
        }

        @Override
        public CalculatedValue<NullSupportingData> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            final HistogramStatistic.HistogramAccumulator calculator =
                    (HistogramStatistic.HistogramAccumulator) dependencies.get(HISTOGRAM_STATISTIC.get());
            return new CalculatedValue.Builder<NullSupportingData>()
                    .setValue(calculator.calculate(((TPStatistic) getStatistic()).getPercentile()))
                    .build();
        }

        @Override
        public int hashCode() {
            return Objects.hash(getClass(), getStatistic());
        }

        @Override
        public boolean equals(final Object other) {
            if (this == other) {
                return true;
            }

            if (!(other instanceof PercentileCalculator)) {
                return false;
            }

            final PercentileCalculator otherPercentileCalculator = (PercentileCalculator) other;
            return getStatistic().equals(otherPercentileCalculator.getStatistic());
        }
    }
}
