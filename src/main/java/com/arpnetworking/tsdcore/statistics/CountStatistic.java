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

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Counts the entries. Use {@link StatisticFactory} for construction.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@Loggable
public final class CountStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "count";
    }

    @Override
    public Set<String> getAliases() {
        return Collections.singleton("n");
    }

    @Override
    public Calculator<NullSupportingData> createCalculator() {
        return new CountAccumulator(this);
    }

    @Override
    public Quantity calculate(final List<Quantity> unorderedValues) {
        return new Quantity.Builder().setValue((double) unorderedValues.size()).build();
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        double count = 0;
        for (final AggregatedData aggregation : aggregations) {
            count += aggregation.getValue().getValue();
        }
        return new Quantity.Builder().setValue(count).build();
    }

    private CountStatistic() { }

    private static final long serialVersionUID = 983762187313397225L;

    /**
     * Accumulator computing the count of values.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    private static final class CountAccumulator extends BaseCalculator<NullSupportingData> implements Accumulator<NullSupportingData> {

        /**
         * Public constructor.
         *
         * @param statistic The {@link Statistic}.
         */
        private CountAccumulator(final Statistic statistic) {
            super(statistic);
        }

        @Override
        public Accumulator<NullSupportingData> accumulate(final Quantity quantity) {
            ++_count;
            return this;
        }

        @Override
        public Accumulator<NullSupportingData> accumulate(final CalculatedValue<NullSupportingData> calculatedValue) {
            _count += (long) calculatedValue.getValue().getValue();
            return this;
        }

        @Override
        public CalculatedValue<NullSupportingData> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return new CalculatedValue.Builder<NullSupportingData>()
                    .setValue(new Quantity.Builder()
                            .setValue((double) _count)
                            .build())
                    .build();
        }

        private long _count = 0;
    }
}
