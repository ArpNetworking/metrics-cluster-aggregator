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
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.arpnetworking.tsdcore.statistics.HistogramStatistic;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.util.Map;
import java.util.concurrent.CompletionStage;

/**
 * Implementation of {@link Sink} which maps values in one unit to another.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class UnitMappingSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        final ImmutableList.Builder<AggregatedData> dataBuilder = ImmutableList.builder();
        for (final AggregatedData datum : periodicData.getData()) {
            final Serializable supportingData = mapSupportingData(datum.getSupportingData());
            final Quantity value = mapQuantity(datum.getValue());
            dataBuilder.add(
                    AggregatedData.Builder.<AggregatedData, AggregatedData.Builder>clone(datum)
                            .setValue(value)
                            .setSupportingData(supportingData)
                            .build());
        }

        final ImmutableList.Builder<Condition> conditionsBuilder = ImmutableList.builder();
        for (final Condition condition : periodicData.getConditions()) {
            final Quantity threshold = condition.getThreshold();
            if (threshold.getUnit().isPresent()) {
                final Unit fromUnit = threshold.getUnit().get();
                final Unit toUnit = _map.get(fromUnit);
                if (toUnit != null) {
                    conditionsBuilder.add(
                            Condition.Builder.<Condition, Condition.Builder>clone(condition)
                                    .setThreshold(new Quantity.Builder()
                                            .setValue(toUnit.convert(threshold.getValue(), fromUnit))
                                            .setUnit(toUnit)
                                            .build())
                                    .build());
                } else {
                    conditionsBuilder.add(condition);
                }
            } else {
                conditionsBuilder.add(condition);
            }
        }
        _sink.recordAggregateData(
                PeriodicData.Builder.clone(periodicData, new PeriodicData.Builder())
                        .setData(dataBuilder.build())
                        .setConditions(conditionsBuilder.build())
                        .build());
    }

    @Override
    public void close() {
        _sink.close();
    }

    @Override
    public CompletionStage<Void> shutdownGracefully() {
        return _sink.shutdownGracefully();
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
                .put("sink", _sink)
                .put("map", _map)
                .build();
    }

    private Quantity mapQuantity(final Quantity quantity) {
        if (quantity.getUnit().isPresent()) {
            final Unit fromUnit = quantity.getUnit().get();
            final Unit toUnit = _map.get(fromUnit);
            if (toUnit != null) {
                return new Quantity.Builder()
                        .setValue(toUnit.convert(quantity.getValue(), fromUnit))
                        .setUnit(toUnit)
                        .build();
            }
        }
        return quantity;
    }

    private Serializable mapSupportingData(final Serializable supportingData) {
        if (supportingData instanceof HistogramStatistic.HistogramSupportingData) {
            final HistogramStatistic.HistogramSupportingData hsd = (HistogramStatistic.HistogramSupportingData) supportingData;
            if (hsd.getUnit().isPresent()) {
                final Unit toUnit = _map.get(hsd.getUnit().get());
                if (toUnit != null) {
                    return hsd.toUnit(toUnit);
                }
            }
        }
        return supportingData;
    }

    private UnitMappingSink(final Builder builder) {
        super(builder);
        _map = Maps.newHashMap(builder._map);
        _sink = builder._sink;
    }

    private final Map<Unit, Unit> _map;
    private final Sink _sink;

    /**
     * Implementation of builder pattern for {@link UnitMappingSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, UnitMappingSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(UnitMappingSink::new);
        }

        /**
         * The map of unit to unit. Cannot be null.
         *
         * @param value The map of unit to unit.
         * @return This instance of {@link Builder}.
         */
        public Builder setMap(final Map<Unit, Unit> value) {
            _map = value;
            return self();
        }

        /**
         * The sink to wrap. Cannot be null.
         *
         * @param value The sink to wrap.
         * @return This instance of {@link Builder}.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Map<Unit, Unit> _map;
        @NotNull
        private Sink _sink;
    }
}
