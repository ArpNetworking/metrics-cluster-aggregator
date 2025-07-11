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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

/**
 * Serves as a data class for storing data for aggregated values after
 * computation.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class AggregatedData implements Serializable {

    /**
     * Gets the fully qualified data space name.
     *
     * @return The fully qualified data space name (FQDSN).
     */
    public FQDSN getFQDSN() {
        return _fqdsn;
    }

    /**
     * @return Period.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public Duration getPeriod() {
        return _period;
    }

    /**
     * @return Host.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public String getHost() {
        return _host;
    }

    /**
     * @return Period Start.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public ZonedDateTime getPeriodStart() {
        return getStart();
    }

    /**
     * @return Period Start.
     * @deprecated Migrate to PeriodicData.
     */
    @Deprecated
    public ZonedDateTime getStart() {
        return _start;
    }

    /**
     * Determines whether this aggregated data was explicitly specified.
     *
     * @return True if the data was explicitly specified, false otherwise.
     */
    public boolean isSpecified() {
        return _isSpecified;
    }

    /**
     * Gets the aggregated value.
     *
     * @return The aggregated value as a Quantity.
     */
    public Quantity getValue() {
        return _value;
    }

    /**
     * Gets the list of samples used in the aggregation.
     *
     * @return The list of samples as Quantities.
     */
    public List<Quantity> getSamples() {
        return _samples;
    }

    public long getPopulationSize() {
        return _populationSize;
    }

    public Serializable getSupportingData() {
        return _supportingData;
    }

    /**
     * Create a fully qualified statistic name (FQSN).
     *
     * @param data The {@link AggregatedData} instance.
     * @return The FQSN.
     */
    public static FQSN createFQSN(final AggregatedData data) {
        // TODO(vkoskela): This is a temporary measure to aid with migrating [MAI-448]
        // away from FQSN data on the AggregatedData instance until FQSN
        // instances are plumbed throughout the codebase.
        return new FQSN.Builder()
                .fromFQDSN(data._fqdsn)
                .setPeriod(data._period)
                .setStart(data._start)
                //.addDimension("host", data._host)
                .build();
    }

    /**
     * Create a fully qualified data space name (FQDSN).
     *
     * @param data The {@link AggregatedData} instance.
     * @return The FQDSN.
     */
    public static FQDSN createFQDSN(final AggregatedData data) {
        // TODO(vkoskela): This is a temporary measure to aid with migrating [MAI-448]
        // away from FQDSN data on the AggregatedData instance until FQDSN
        // instances are plumbed throughout the codebase.
        return data._fqdsn;
    }

    @Override
    public boolean equals(final Object object) {
        if (this == object) {
            return true;
        }
        if (object == null || getClass() != object.getClass()) {
            return false;
        }

        final AggregatedData other = (AggregatedData) object;

        return Objects.equal(_value, other._value)
                && Long.compare(_populationSize, other._populationSize) == 0
                && Objects.equal(_start, other._start)
                && Objects.equal(_period, other._period)
                && Objects.equal(_fqdsn, other._fqdsn)
                && Objects.equal(_host, other._host)
                && Objects.equal(_isSpecified, other._isSpecified)
                && Objects.equal(_samples, other._samples)
                && Objects.equal(_supportingData, other._supportingData);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(
                getFQDSN(),
                getValue(),
                getStart(),
                getPeriod(),
                getHost(),
                getSamples(),
                getPopulationSize(),
                isSpecified(),
                getSupportingData());
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * NOTE: This class is not marked @Loggable due to the potentially large
     * number of samples in the _samples field.  Using @Loggable would cause them
     * all to be serialized and in the past has caused significant performance
     * problems.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("fqdsn", _fqdsn)
                .put("value", _value)
                .put("samplesSize", _samples.size())
                .put("populationSize", _populationSize)
                .put("period", _period)
                .put("start", _start)
                .put("host", _host)
                .put("isSpecified", _isSpecified)
                .build();
    }

    private AggregatedData(final Builder builder) {
        _fqdsn = builder._fqdsn;
        _value = builder._value;
        if (builder._samples instanceof ImmutableList) {
            _samples = (ImmutableList<Quantity>) builder._samples;
        } else {
            _samples = ImmutableList.copyOf(builder._samples);
        }
        _populationSize = builder._populationSize;
        _period = builder._period;
        _start = builder._start;
        _host = builder._host;
        _isSpecified = builder._isSpecified;
        _supportingData = builder._supportingData;
    }

    private final FQDSN _fqdsn;
    private final Quantity _value;
    private final long _populationSize;
    private final ImmutableList<Quantity> _samples;
    private final ZonedDateTime _start;
    private final Duration _period;
    private final String _host;
    private final boolean _isSpecified;
    private final Serializable _supportingData;

    private static final long serialVersionUID = 9124136139360447095L;

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link AggregatedData}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends OvalBuilder<AggregatedData> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregatedData::new);
        }

        /**
         * The fully qualified data space name ({@link FQDSN}). Required. Cannot be null.
         *
         * @param value The {@link FQDSN}.
         * @return This instance of {@link Builder}.
         */
        public Builder setFQDSN(final FQDSN value) {
            _fqdsn = value;
            return this;
        }

        /**
         * The value. Required. Cannot be null.
         *
         * @param value The value.
         * @return This instance of {@link Builder}.
         */
        public Builder setValue(final Quantity value) {
            _value = value;
            return this;
        }

        /**
         * The samples. Required. Cannot be null.
         *
         * @param value The samples.
         * @return This instance of {@link Builder}.
         */
        public Builder setSamples(final Collection<Quantity> value) {
            _samples = value;
            return this;
        }

        /**
         * The population size. Required. Cannot be null.
         *
         * @param value The samples.
         * @return This instance of {@link Builder}.
         */
        public Builder setPopulationSize(final Long value) {
            _populationSize = value;
            return this;
        }

        /**
         * The period start. Required. Cannot be null.
         *
         * @param value The period start.
         * @return This instance of {@link Builder}.
         */
        public Builder setStart(final ZonedDateTime value) {
            _start = value;
            return this;
        }

        /**
         * The period. Required. Cannot be null.
         *
         * @param value The period.
         * @return This instance of {@link Builder}.
         */
        public Builder setPeriod(final Duration value) {
            _period = value;
            return this;
        }

        /**
         * The host. Required. Cannot be null or empty.
         *
         * @param value The host.
         * @return This instance of {@link Builder}.
         */
        public Builder setHost(final String value) {
            _host = value;
            return this;
        }

        /**
         * The aggregated data was specified. Required. Cannot be null.
         *
         * @param value The metric type.
         * @return This instance of {@link Builder}.
         */
        public Builder setIsSpecified(final Boolean value) {
            _isSpecified = value;
            return this;
        }

        /**
         * The supporting data.
         *
         * @param value The supporting data.
         * @return This instance of {@link Builder}.
         */
        public Builder setSupportingData(final Serializable value) {
            _supportingData = value;
            return this;
        }

        @NotNull
        private FQDSN _fqdsn;
        @NotNull
        private Quantity _value;
        @NotNull
        private Collection<Quantity> _samples = Collections.emptyList();
        @NotNull
        private Long _populationSize;
        @NotNull
        private ZonedDateTime _start;
        @NotNull
        private Duration _period;
        @NotNull
        @NotEmpty
        private String _host;
        @NotNull
        private Boolean _isSpecified;
        private Serializable _supportingData;
    }
}
