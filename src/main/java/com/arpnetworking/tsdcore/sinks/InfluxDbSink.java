/*
 * Copyright 2016 Groupon.com
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

import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;
import org.apache.pekko.http.javadsl.model.HttpMethods;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.StringJoiner;
import javax.annotation.Nonnull;

/**
 * Publishes to a InfluxDB endpoint. This class is thread safe.
 *
 * @author Daniel Guerrero (dguerreromartin at groupon dot com)
 */
public final class InfluxDbSink extends HttpPostSink {


    @Override
    protected RequestInfo createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        final Request request = new RequestBuilder()
                .setUri(getAysncHttpClientUri())
                .setHeader("Content-Type", "application/x-www-form-urlencoded; charset=UTF-8")
                .setBody(serializedData)
                .setMethod(HttpMethods.POST.value())
                .build();
        return new RequestInfo(request, serializedData.length, serializedData.length);
    }

    @Override
    protected Collection<SerializedDatum> serialize(final PeriodicData periodicData) {
        final String period = periodicData.getPeriod()
            .toString();

        final Map<String, MetricFormat> metrics = Maps.newHashMap();

        for (final AggregatedData data : periodicData.getData()) {
            final String metricName = buildMetricName(period, data.getFQDSN());
            MetricFormat formattedData = metrics.get(metricName);

            if (formattedData == null) {
                formattedData = new MetricFormat(
                        metricName,
                        periodicData.getStart().toInstant().toEpochMilli(),
                        periodicData.getDimensions()
                )
                        .addTag("service", data.getFQDSN().getService())
                        .addTag("cluster", data.getFQDSN().getCluster());

                metrics.put(metricName, formattedData);
            }

            formattedData.addMetric(
                    data.getFQDSN().getStatistic().getName(),
                    data.getValue().getValue()
            );
        }

        final List<SerializedDatum> requests = Lists.newArrayList();
        int currentRequestLineCount = 0;
        StringJoiner currentRequestData = new StringJoiner("\n");
        for (final MetricFormat metric : metrics.values()) {
            currentRequestData.add(metric.buildMetricString());
            ++currentRequestLineCount;
            if (currentRequestLineCount >= _linesPerRequest) {
                requests.add(new SerializedDatum(
                        currentRequestData.toString().getBytes(StandardCharsets.UTF_8),
                        Optional.empty()));
                currentRequestLineCount = 0;
                currentRequestData = new StringJoiner("\n");
            }
        }
        if (currentRequestLineCount > 0) {
            requests.add(new SerializedDatum(
                    currentRequestData.toString().getBytes(StandardCharsets.UTF_8),
                    Optional.empty()));
        }

        return requests;
    }


    private String buildMetricName(final String period, final FQDSN fqdsn) {
        return new StringBuilder()
                .append(period).append(".")
                .append(fqdsn.getMetric())
                .toString();
    }

    /**
     * Private constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    private InfluxDbSink(final Builder builder) {
        super(builder);
        _linesPerRequest = builder._linesPerRequest;
    }

    private final long _linesPerRequest;

    /**
     * Implementation of output format for InfluxDB metrics.
     * The format follow the pattern (https://docs.influxdata.com/influxdb/v0.10/write_protocols/write_syntax/):
     * {@code
     *      measurement[,tag_key1=tag_value1...] field_key=field_value[,field_key2=field_value2] [timestamp]
     * }
     * The spaces, comma and = will be escaped from the measurement,tags and fields
     *
     * @author Daniel Guerrero (dguerreromartin at groupon dot com)
     */
    private static class MetricFormat {

        public MetricFormat addTag(final String tagName, final String value) {
            this._tags.put(encode(tagName), encode(value));
            return this;
        }

        public MetricFormat addMetric(final String statisticName, final Double value) {
            this._values.put(encode(statisticName), value);
            return this;
        }

        public String buildMetricString() {
            final StringJoiner metricName = new StringJoiner(",");
            metricName.add(_metric);

            for (final Map.Entry<String, String> entryTag : this._tags.entrySet()) {
                metricName.add(String.format("%s=%s", entryTag.getKey(), entryTag.getValue()));
            }

            final StringJoiner valuesJoiner = new StringJoiner(",");

            for (final Map.Entry<String, Double> entryValue : this._values.entrySet()) {
                valuesJoiner.add(String.format("%s=%s", entryValue.getKey(), entryValue.getValue()));
            }

            return String.format("%s %s %d", metricName.toString(), valuesJoiner.toString(), _timestamp);
        }

        MetricFormat(final String metric, final long timestamp, final Map<String, String> tags) {
            this._metric = encode(metric);
            this._timestamp = timestamp;
            for (final Map.Entry<String, String> tag : tags.entrySet()) {
                this._tags.put(encode(tag.getKey()), encode(tag.getValue()));
            }
        }

        private String encode(final String name) {
            return name
                    .replace(",", "\\,")
                    .replace(" ", "\\ ")
                    .replace("=", "_");
        }

        private final String _metric;
        private final long _timestamp;
        private final Map<String, Double> _values = Maps.newHashMap();
        private final Map<String, String> _tags = Maps.newHashMap();

    }

    /**
     * Implementation of builder pattern for {@link InfluxDbSink}.
     *
     * @author Daniel Guerrero (dguerreromartin at groupon dot com)
     */
    public static final class Builder extends HttpPostSink.Builder<Builder, InfluxDbSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(InfluxDbSink::new);
        }

        /**
         * Sets maximum lines per request. Optional. Defaults to 10000. Cannot be null or less than 1.
         *
         * @param value The lines per request.
         * @return This instance of {@link Builder}.
         */
        public Builder setLinesPerRequest(@Nonnull final Long value) {
            _linesPerRequest = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        @Min(1)
        private Long _linesPerRequest = 10000L;
    }

}
