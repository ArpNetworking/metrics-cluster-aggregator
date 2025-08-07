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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import net.sf.oval.constraint.NotNull;
import org.apache.pekko.http.javadsl.model.HttpMethods;
import org.apache.pekko.http.javadsl.model.MediaTypes;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;

import java.nio.charset.Charset;
import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Publishes aggregations to KMonD. This class is thread safe.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class KMonDSink extends HttpPostSink {

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
                .put("severityToStatus", _severityToStatus)
                .put("unknownSeverityStatus", _unknownSeverityStatus)
                .build();
    }

    @Override
    protected Collection<SerializedDatum> serialize(final PeriodicData periodicData) {
        final Duration period = periodicData.getPeriod();
        final Multimap<String, AggregatedData> indexedData = prepareData(periodicData);
        final Multimap<String, Condition> indexedConditions = prepareConditions(periodicData.getConditions());

        // Serialize
        final List<SerializedDatum> serializedData = Lists.newArrayListWithCapacity(indexedData.size());
        final StringBuilder stringBuilder = new StringBuilder();
        for (final String key : indexedData.keySet()) {
            final Collection<AggregatedData> namedData = indexedData.get(key);
            if (!namedData.isEmpty()) {
                stringBuilder.setLength(0);
                final AggregatedData first = Iterables.getFirst(namedData, null);
                final String name = new StringBuilder()
                        .append(first.getFQDSN().getService())
                        .append("_")
                        .append(period.toString())
                        .append("_")
                        .append(first.getFQDSN().getMetric())
                        .toString();

                int maxStatus = 0;
                boolean hasAlert = false;
                final StringBuilder dataBuilder = new StringBuilder();
                for (final AggregatedData datum : namedData) {
                    if (!datum.isSpecified()) {
                        continue;
                    }

                    dataBuilder.append(datum.getFQDSN().getStatistic().getName())
                            .append("%3D").append(datum.getValue().getValue())
                            .append("%3B");

                    final String conditionKey = datum.getFQDSN().getService() + "_"
                            + datum.getFQDSN().getMetric() + "_"
                            + datum.getFQDSN().getCluster() + "_"
                            + datum.getFQDSN().getStatistic();
                    for (final Condition condition : indexedConditions.get(conditionKey)) {
                        hasAlert = true;
                        maxStatus = serializeCondition(maxStatus, dataBuilder, datum, condition);
                    }
                }

                // Don't send an empty payload
                if (dataBuilder.length() == 0) {
                    continue;
                }

                stringBuilder.append("run_every=").append(period.getSeconds())
                        .append("&has_alert=").append(hasAlert)
                        .append("&path=").append(first.getFQDSN().getCluster())
                        .append("%2f").append(periodicData.getDimensions().get("host"))
                        .append("&monitor=").append(name)
                        .append("&status=").append(maxStatus)
                        .append("&timestamp=").append((int) Unit.SECOND.convert(
                                periodicData.getStart().toInstant().toEpochMilli(),
                                Unit.MILLISECOND))
                        .append("&output=").append(name)
                        .append("%7C")
                        .append(dataBuilder.toString());

                stringBuilder.setLength(stringBuilder.length() - 3);
                serializedData.add(new SerializedDatum(
                        stringBuilder.toString().getBytes(Charset.forName("UTF-8")),
                        Optional.empty()));
            }
        }

        return serializedData;
    }

    @Override
    protected RequestInfo createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        final Request request = new RequestBuilder()
                .setUri(getAysncHttpClientUri())
                .setHeader("Content-Type", MediaTypes.APPLICATION_X_WWW_FORM_URLENCODED.toString())
                .setBody(serializedData)
                .setMethod(HttpMethods.POST.value())
                .build();
        return new RequestInfo(request, serializedData.length, serializedData.length);
    }

    private int serializeCondition(
            final int maxStatus,
            final StringBuilder dataBuilder,
            final AggregatedData datum,
            final Condition condition) {

        int newMaxStatus = maxStatus;
        dataBuilder.append(datum.getFQDSN().getStatistic().getName())
                .append("_").append(condition.getName())
                .append("%3D").append(condition.getThreshold().getValue())
                .append("%3B");

        if (condition.isTriggered().isPresent() && condition.isTriggered().get()) {
            // Collect the status of this metric
            final Object severity = condition.getExtensions().get("severity");
            int status = _unknownSeverityStatus;
            if (severity != null && _severityToStatus.containsKey(severity)) {
                status = _severityToStatus.get(severity);
            }
            newMaxStatus = Math.max(status, newMaxStatus);
        }
        return newMaxStatus;
    }

    private Multimap<String, Condition> prepareConditions(final Collection<Condition> conditions) {
        // Transform conditions
        return Multimaps.index(
                conditions,
                input -> {
                    // NOTE: It is assumed as part of serialization that
                    // that period is part of the unique metric name.
                    if (input == null) {
                        return null;
                    }
                    return input.getFQDSN().getService() + "_"
                            + input.getFQDSN().getMetric() + "_"
                            + input.getFQDSN().getCluster() + "_"
                            + input.getFQDSN().getStatistic();
                });
    }

    private Multimap<String, AggregatedData> prepareData(final PeriodicData periodicData) {
        // Transform the data list to a multimap by metric name
        // Ie, get all the statistics for a unique metric

        return Multimaps.index(
                periodicData.getData(), input ->
                        input.getFQDSN().getService() + "_"
                                + periodicData.getPeriod().toString() + "_"
                                + input.getFQDSN().getMetric() + "_"
                                + periodicData.getDimensions().get("host") + "_"
                                + input.getFQDSN().getCluster());
    }

    private KMonDSink(final Builder builder) {
        super(builder);
        _severityToStatus = Maps.newHashMap(builder._severityToStatus);
        _unknownSeverityStatus = builder._unknownSeverityStatus;
    }

    private final Map<String, Integer> _severityToStatus;
    private final int _unknownSeverityStatus;

    /**
     * Implementation of builder pattern for {@link MonitordSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends HttpPostSink.Builder<Builder, KMonDSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(KMonDSink::new);
        }

        /**
         * Set severity to status map. Optional. Cannot be null. By default is
         * an {@link Map} containing the following:
         *
         * {@code
         * "warning" => 1
         * "critical" => 2
         * }
         *
         * @param value Map of severity to status.
         * @return This {@link Builder} instance.
         */
        public Builder setSeverityToStatus(final Map<String, Integer> value) {
            _severityToStatus = value;
            return self();
        }

        /**
         * The status for unknown {@link Condition} severities; e.g. those
         * not found in the severity to status map. Optional. Cannot be null.
         * By default the status for a {@link Condition} is {@code 2}.
         *
         * @param value Default status.
         * @return This {@link Builder} instance.
         */
        public Builder setUnknownSeverityStatus(final Integer value) {
            _unknownSeverityStatus = value;
            return self();
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private Map<String, Integer> _severityToStatus = ImmutableMap.of(
                "warning", 1,
                "critical", 2);
        @NotNull
        private Integer _unknownSeverityStatus = 2;
    }
}
