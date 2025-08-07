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

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.apache.pekko.http.javadsl.model.HttpMethods;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * Publishes aggregations to Data Dog. This class is thread safe.
 *
 * API Documentation:
 * http://docs.datadoghq.com/api/
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class DataDogSink extends HttpPostSink {

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
                .put("apiKey", _apiKey)
                .build();
    }

    @Override
    protected Collection<SerializedDatum> serialize(final PeriodicData periodicData) {
        final String period = periodicData.getPeriod().toString();
        final long timestamp = (periodicData.getStart().toInstant().toEpochMilli()
                + periodicData.getPeriod().toMillis()) / 1000;

        final List<Datum> dataDogData = Lists.newArrayList();
        for (final AggregatedData datum : periodicData.getData()) {
            if (!datum.isSpecified()) {
                continue;
            }

            dataDogData.add(new Datum(
                    period + "_" + datum.getFQDSN().getMetric() + "_" + datum.getFQDSN().getStatistic().getName(),
                    timestamp,
                    (float) datum.getValue().getValue(),
                    periodicData.getDimensions().get("host"),
                    createTags(periodicData, datum)));
        }

        final String dataDogDataAsJson;
        try {
            dataDogDataAsJson = OBJECT_MAPPER.writeValueAsString(
                    Collections.singletonMap("series", dataDogData));
        } catch (final JsonProcessingException e) {
            LOGGER.error()
                    .setMessage("Serialization error")
                    .addData("periodicData", periodicData)
                    .setThrowable(e)
                    .log();
            return Collections.emptyList();
        }
        return Collections.singletonList(new SerializedDatum(
                dataDogDataAsJson.getBytes(StandardCharsets.UTF_8),
                Optional.empty()));
    }

    private static List<String> createTags(final PeriodicData periodicData, final AggregatedData datum) {
        final List<String> tags = Lists.newArrayList();
        // TODO(vkoskela): The publication of cluster vs host metrics needs to be formalized. [AINT-678]
        final String host = periodicData.getDimensions().get("host");
        if (host == null) {
            tags.add("scope:" + "host");
            tags.add("host:unknown");
        } else if (host.matches("^.*-cluster\\.[^.]+$")) {
            tags.add("scope:" + "cluster");
        } else {
            tags.add("scope:" + "host");
            tags.add("host:" + host);
        }
        tags.add("service:" + datum.getFQDSN().getService());
        tags.add("cluster:" + datum.getFQDSN().getCluster());
        return tags;
    }

    @Override
    protected RequestInfo createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        final Request request = new RequestBuilder()
                .setUri(getAysncHttpClientUri())
                .setBody(serializedData)
                .setMethod(HttpMethods.POST.value())
                .setHeader("Content-Type", "application/json")
                .addQueryParam("api_key", _apiKey)
                .build();
        return new RequestInfo(request, serializedData.length, serializedData.length);
    }

    private DataDogSink(final Builder builder) {
        super(builder);
        _apiKey = builder._apiKey;
    }

    private final String _apiKey;

    private static final Logger LOGGER = LoggerFactory.getLogger(DataDogSink.class);
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    private static final class Datum {

        private Datum(
                final String metric,
                final long secondsSinceEpoch,
                final float value,
                final String host,
                final List<String> tags) {
            _metric = metric;
            _host = host;
            _tags = tags;
            _points = Collections.singletonList(Arrays.asList(secondsSinceEpoch, value));
        }

        public String getMetric() {
            return _metric;
        }

        public String getHost() {
            return _host;
        }

        public List<String> getTags() {
            return _tags;
        }

        public List<Object> getPoints() {
            return _points;
        }

        private final String _metric;
        private final String _host;
        private final List<String> _tags;
        private final List<Object> _points;
    }

    /**
     * Implementation of builder pattern for {@link DataDogSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends HttpPostSink.Builder<Builder, DataDogSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DataDogSink::new);
        }

        /**
         * The API key. Required. Cannot be null or empty.
         *
         * @param value API key.
         * @return This {@link Builder} instance.
         */
        public Builder setApiKey(final String value) {
            _apiKey = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        @NotEmpty
        private String _apiKey;
    }
}
