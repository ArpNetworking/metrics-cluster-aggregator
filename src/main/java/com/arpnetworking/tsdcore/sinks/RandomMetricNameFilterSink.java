/*
 * Copyright 2014 Groupon.com
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableList;
import net.sf.oval.constraint.Max;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;

/**
 * A {@link com.arpnetworking.tsdcore.sinks.Sink} that only allows a percentage of data through to the wrapped
 * {@link com.arpnetworking.tsdcore.sinks.Sink}.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class RandomMetricNameFilterSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        final ImmutableList.Builder<AggregatedData> filteredDataBuilder = ImmutableList.builder();
        periodicData.getData()
                .stream()
                .filter(this::shouldPass)
                .forEach(filteredDataBuilder::add);

        _sink.recordAggregateData(
                PeriodicData.Builder.clone(periodicData, new PeriodicData.Builder())
                        .setData(filteredDataBuilder.build())
                        .build());
    }

    @Override
    public void close() {
        _sink.close();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("passPercent", _passPercent)
                .put("sink", _sink)
                .build();
    }


    /**
     * Determines if an {@link com.arpnetworking.tsdcore.model.AggregatedData} should be sent to the wrapped Sink.
     *
     * @param datum Data to check
     * @return true to pass the datum, false to drop it
     */
    /* package private*/ boolean shouldPass(final AggregatedData datum) {
        final String name = new StringBuilder()
                .append(datum.getFQDSN().getCluster())
                .append(datum.getFQDSN().getMetric())
                .append(datum.getFQDSN().getStatistic())
                .append(datum.getFQDSN().getService())
                .toString();
        final int mod = name.hashCode() % 100;
        return mod < _passPercent && mod > (-1 * _passPercent);
    }

    private RandomMetricNameFilterSink(final Builder builder) {
        super(builder);
        _passPercent = builder._passPercent;
        _sink = builder._sink;
    }

    private final int _passPercent;
    private final Sink _sink;

    /**
     * Implementation of the builder pattern for {@link RandomMetricNameFilterSink}.
     */
    public static class Builder extends BaseSink.Builder<Builder, RandomMetricNameFilterSink> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(RandomMetricNameFilterSink::new);
        }

        /**
         * The aggregated data sink to limit. Cannot be null.
         * Required.
         *
         * @param value The aggregated data sink to limit.
         * @return This instance of {@link Builder}.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        /**
         * Return this instance of {@link Builder} as this sub-type.
         *
         * @return This instance of {@link Builder}.
         */
        public Builder self() {
            return this;
        }

        /**
         * The percentage of data to pass to the wrapped sink.
         * Required.
         *
         * @param value The percentage to pass (0 - 100)
         * @return This instance of {@link Builder}.
         */
        public Builder setPassPercent(final Integer value) {
            _passPercent = value;
            return this;
        }

        @NotNull
        private Sink _sink;
        @Min(0)
        @Max(100)
        @NotNull
        private Integer _passPercent;
    }
}
