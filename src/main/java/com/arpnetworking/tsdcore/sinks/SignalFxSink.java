/*
 * Copyright 2015 Groupon.com
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

import akka.http.javadsl.model.HttpMethods;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.Lists;
import com.signalfx.metrics.protobuf.SignalFxProtocolBuffers;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import javax.annotation.Nonnull;

/**
 * Publishes aggregations to Signal FX. This class is thread safe.
 *
 * Documentation:
 * https://support.signalfx.com/hc/en-us
 *
 * Signal FX REST API Article:
 * https://support.signalfx.com/hc/en-us/articles/201270489-Use-the-SignalFx-REST-API
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class SignalFxSink extends HttpPostSink {

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
                .put("source", _source)
                .put("organizationId", _organizationId)
                .put("apiToken", _apiToken)
                .put("maxMetricDimensions", _maxMetricDimensions)
                .build();
    }

    @Override
    protected Collection<SerializedDatum> serialize(final PeriodicData periodicData) {
        final String period = periodicData.getPeriod().toString();
        final long timestamp = periodicData.getStart().toInstant().toEpochMilli()
                + periodicData.getPeriod().toMillis();

        final List<SerializedDatum> serializedData = Lists.newArrayList();
        SignalFxProtocolBuffers.DataPointUploadMessage.Builder sfxMessage = SignalFxProtocolBuffers.DataPointUploadMessage.newBuilder();
        int count = 0;
        for (final AggregatedData datum : periodicData.getData()) {
            if (!datum.isSpecified()) {
                continue;
            }

            final List<SignalFxProtocolBuffers.Dimension> sfxDimensions = createDimensions(periodicData, datum);
            final SignalFxProtocolBuffers.DataPoint dataPoint = createDataPoint(period, timestamp, datum, sfxDimensions);
            sfxMessage.addDatapoints(dataPoint);

            // In conversation with the SignalFX team we were instructed to limit the number of data points sent per
            // request based on the data points and dimensions per data point.
            count += Math.max(1, sfxDimensions.size());

            if (count >= _maxMetricDimensions) {
                serializedData.add(new SerializedDatum(sfxMessage.build().toByteArray(), Optional.empty()));
                sfxMessage = SignalFxProtocolBuffers.DataPointUploadMessage.newBuilder();
                count = 0;
            }
        }

        if (count > 0) {
            serializedData.add(new SerializedDatum(sfxMessage.build().toByteArray(), Optional.empty()));
        }
        return serializedData;
    }

    @Override
    protected Request createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        return new RequestBuilder()
                .setUri(getAysncHttpClientUri())
                .setBody(serializedData)
                .setMethod(HttpMethods.POST.value())
                .setHeader("Content-Type", "application/x-protobuf")
                .setHeader("Content-Length", String.valueOf(serializedData.length))
                .setHeader("Accept", "application/json")
                .setHeader("X-SF-TOKEN", _apiToken)
                .addQueryParam("orgid", _organizationId)
                .build();
    }

    private SignalFxProtocolBuffers.DataPoint createDataPoint(
            final String period,
            final long timestamp,
            final AggregatedData datum,
            final List<SignalFxProtocolBuffers.Dimension> sfxDimensions) {
        final SignalFxProtocolBuffers.Datum.Builder sfxDatum = SignalFxProtocolBuffers.Datum.newBuilder();
        sfxDatum.setDoubleValue(datum.getValue().getValue());
        final SignalFxProtocolBuffers.DataPoint.Builder sfxDataPoint = SignalFxProtocolBuffers.DataPoint.newBuilder();
        sfxDataPoint.setMetricType(SignalFxProtocolBuffers.MetricType.GAUGE);
        sfxDataPoint.setMetric(period + "_" + datum.getFQDSN().getMetric() + "_" + datum.getFQDSN().getStatistic().getName());
        sfxDataPoint.addAllDimensions(sfxDimensions);
        sfxDataPoint.setTimestamp(timestamp);
        sfxDataPoint.setValue(sfxDatum);
        sfxDataPoint.setSource(_source.orElse(null));
        return sfxDataPoint.build();
    }

    private List<SignalFxProtocolBuffers.Dimension> createDimensions(final PeriodicData periodicData, final AggregatedData datum) {
        final List<SignalFxProtocolBuffers.Dimension> sfxDimensions = Lists.newArrayList();
        // TODO(vkoskela): The publication of cluster vs host metrics needs to be formalized. [AINT-678]
        final String host = periodicData.getDimensions().get("host");
        if (host == null) {
            sfxDimensions.add(createSfxDimension("scope", "host"));
            sfxDimensions.add(createSfxDimension("host", "unknown"));
        } else if (host.matches("^.*-cluster\\.[^.]+$")) {
            sfxDimensions.add(createSfxDimension("scope", "cluster"));
        } else {
            sfxDimensions.add(createSfxDimension("scope", "host"));
            sfxDimensions.add(createSfxDimension("host", host));
        }
        sfxDimensions.add(createSfxDimension("service", datum.getFQDSN().getService()));
        sfxDimensions.add(createSfxDimension("cluster", datum.getFQDSN().getCluster()));
        return sfxDimensions;
    }

    private static SignalFxProtocolBuffers.Dimension createSfxDimension(final String key, final String value) {
        final SignalFxProtocolBuffers.Dimension.Builder sfxDimension = SignalFxProtocolBuffers.Dimension.newBuilder();
        sfxDimension.setKey(key);
        sfxDimension.setValue(value);
        return sfxDimension.build();
    }

    private SignalFxSink(final Builder builder) {
        super(builder);
        _source = Optional.ofNullable(builder._source);
        _organizationId = builder._organizationId;
        _apiToken = builder._apiToken;
        _maxMetricDimensions = builder._maxMetricDimensions;
    }

    private final Optional<String> _source;
    private final String _organizationId;
    private final String _apiToken;
    private final int _maxMetricDimensions;

    /**
     * Implementation of builder pattern for {@link SignalFxSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends HttpPostSink.Builder<Builder, SignalFxSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(SignalFxSink::new);
        }

        /**
         * The source name. Optional.
         *
         * @param value Organization identifier.
         * @return This {@link Builder} instance.
         */
        public Builder setSource(final String value) {
            _source = value;
            return self();
        }

        /**
         * The organization identifier. Listed under the "Catalog" page at
         * www.signalfx.com. Required. Cannot be null or empty.
         *
         * @param value Organization identifier.
         * @return This {@link Builder} instance.
         */
        public Builder setOrganizationId(final String value) {
            _organizationId = value;
            return self();
        }

        /**
         * The API token. Listed under each organization user's profile at
         * www.signalfx.com. Required. Cannot be null or empty.
         *
         * @param value API token.
         * @return This {@link Builder} instance.
         */
        public Builder setApiToken(final String value) {
            _apiToken = value;
            return self();
        }

        /**
         * Sets the maximum metric-dimensions per request.
         * Optional. Defaults to 1000.
         *
         * @param value the maximum metric-dimensions per request.
         * @return This instance of {@link Builder}.
         */
        public Builder setMaxMetricDimensions(final Integer value) {
            _maxMetricDimensions = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        private String _source;
        @NotNull
        @NotEmpty
        private String _organizationId;
        @NotNull
        @NotEmpty
        private String _apiToken;
        @NotNull
        @Min(value = 0)
        private Integer _maxMetricDimensions = 1000;
    }
}
