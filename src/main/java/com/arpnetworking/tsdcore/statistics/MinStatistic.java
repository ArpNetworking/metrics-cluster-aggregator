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

import com.arpnetworking.logback.annotations.Loggable;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.google.common.collect.Sets;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * Min statistic (e.g. top 0th percentile). Use {@link StatisticFactory} for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class MinStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "min";
    }

    @Override
    public Set<String> getAliases() {
        return ALIASES;
    }

    @Override
    public Calculator<NullSupportingData> createCalculator() {
        return new MinAccumulator(this);
    }

    @Override
    public Quantity calculate(final List<Quantity> unorderedValues) {
        Optional<Quantity> min = Optional.empty();
        for (final Quantity sample : unorderedValues) {
            if (min.isPresent()) {
                if (sample.compareTo(min.get()) < 0) {
                    min = Optional.of(sample);
                }
            } else {
                min = Optional.of(sample);
            }
        }
        return min.get();
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        Optional<Quantity> min = Optional.empty();
        for (final AggregatedData datum : aggregations) {
            if (min.isPresent()) {
                if (datum.getValue().compareTo(min.get()) < 0) {
                    min = Optional.of(datum.getValue());
                }
            } else {
                min = Optional.of(datum.getValue());
            }
        }
        return min.get();
    }

    private MinStatistic() { }

    private static final Set<String> ALIASES;
    private static final long serialVersionUID = 107620025236661457L;

    static {
        ALIASES = Sets.newHashSet();
        ALIASES.add("tp0");
        ALIASES.add("p0");
    }

    /**
     * Accumulator computing the minimum of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class MinAccumulator extends BaseCalculator<NullSupportingData> implements Accumulator<NullSupportingData> {

        /**
         * Public constructor.
         *
         * @param statistic The {@link Statistic}.
         */
        public MinAccumulator(final Statistic statistic) {
            super(statistic);
        }

        @Override
        public Accumulator<NullSupportingData> accumulate(final Quantity quantity) {
            if (_min.isPresent()) {
                if (quantity.compareTo(_min.get()) < 0) {
                    _min = Optional.of(quantity);
                }
            } else {
                _min = Optional.of(quantity);
            }
            return this;
        }

        @Override
        public Accumulator<NullSupportingData> accumulate(final CalculatedValue<NullSupportingData> calculatedValue) {
            return accumulate(calculatedValue.getValue());
        }

        @Override
        public CalculatedValue<NullSupportingData> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return new CalculatedValue.Builder<NullSupportingData>()
                    .setValue(_min.orElse(null))
                    .build();
        }

        private Optional<Quantity> _min = Optional.empty();
    }
}
