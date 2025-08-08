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
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.statistics.HistogramStatistic;
import com.arpnetworking.tsdcore.statistics.MaxStatistic;
import com.arpnetworking.tsdcore.statistics.MeanStatistic;
import com.arpnetworking.tsdcore.statistics.MinStatistic;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.SumStatistic;
import com.fasterxml.jackson.core.JsonEncoding;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import io.netty.handler.codec.http.HttpHeaderNames;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.util.HttpConstants;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.LongAdder;
import java.util.zip.GZIPOutputStream;

/**
 * Publishes to a KairosDbSink endpoint. This class is thread safe.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class KairosDbSink extends HttpPostSink {

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
                .put("maxRequestSize", _maxRequestSize)
                .put("ttlSeconds", _ttlSeconds)
                .put("publishHistograms", _publishHistograms)
                .put("histogramTtlSeconds", _histogramTtlSeconds)
                .build();
    }

    @Override
    protected Collection<SerializedDatum> serialize(final PeriodicData periodicData) {
        // Initialize serialization structures
        final List<SerializedDatum> completeChunks = Lists.newArrayList();
        final ByteBuffer currentChunk = ByteBuffer.allocate(_maxRequestSize);
        final LongAdder currentChunkPopulationSize = new LongAdder();
        final ByteArrayOutputStream chunkStream = new ByteArrayOutputStream();

        // Extract and transform shared data
        final long timestamp = periodicData.getStart().plus(periodicData.getPeriod()).toInstant().toEpochMilli();
        final String serializedPeriod = periodicData.getPeriod().toString();
        final Serializer serializer = new Serializer(timestamp, serializedPeriod, periodicData.getDimensions());

        final KairosHistogramAdditionalData histogramAdditionalData = new KairosHistogramAdditionalData();
        AggregatedData histogram = null;

        // Initialize the chunk buffer
        currentChunk.put(HEADER);

        // Add aggregated data
        for (final AggregatedData datum : periodicData.getData()) {
            final Statistic statistic = datum.getFQDSN().getStatistic();
            if (_publishHistograms) {
                // We need to collect the min, max, mean, and sum
                if (statistic instanceof MeanStatistic) {
                    histogramAdditionalData.setMean(datum.getValue().getValue());
                } else if (statistic instanceof SumStatistic) {
                    histogramAdditionalData.setSum(datum.getValue().getValue());
                } else if (statistic instanceof MaxStatistic) {
                    histogramAdditionalData.setMax(datum.getValue().getValue());
                } else if (statistic instanceof MinStatistic) {
                    histogramAdditionalData.setMin(datum.getValue().getValue());
                } else if (statistic instanceof HistogramStatistic) {
                    histogram = datum;
                }
            }

            if (!datum.isSpecified()) {
                LOGGER.trace()
                        .setMessage("Skipping unspecified datum")
                        .addData("datum", datum)
                        .log();
                continue;
            }

            if (_publishStandardMetrics && !(statistic instanceof HistogramStatistic)) {
                serializer.serializeDatum(completeChunks, currentChunk, chunkStream, datum, currentChunkPopulationSize);
            }
        }

        if (_publishHistograms && histogram != null) {
            serializer.serializeHistogram(
                    completeChunks,
                    currentChunk,
                    chunkStream,
                    histogram,
                    histogramAdditionalData,
                    currentChunkPopulationSize);
        } else if (_publishHistograms) {
            NO_HISTOGRAM_LOGGER.warn()
                    .setMessage("Expected to publish histogram, but none found")
                    .addData("periodicData", periodicData)
                    .log();
        }

        // Add conditions
        for (final Condition condition : periodicData.getConditions()) {
            serializer.serializeCondition(completeChunks, currentChunk, chunkStream, condition, currentChunkPopulationSize);
        }

        // Add the current chunk (if any) to the completed chunks
        if (currentChunk.position() > HEADER_BYTE_LENGTH) {
            currentChunk.put(currentChunk.position() - 1, FOOTER);
            completeChunks.add(new SerializedDatum(
                    Arrays.copyOf(currentChunk.array(), currentChunk.position()),
                    Optional.of(currentChunkPopulationSize.sumThenReset())));
        }

        return completeChunks;
    }

    @Override
    protected RequestInfo createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        if (this.getEnableCompression()) {
            final byte[] bodyData;
            try (ByteArrayOutputStream bos = new ByteArrayOutputStream();
                 GZIPOutputStream gzipStream = new GZIPOutputStream(bos)) {

                gzipStream.write(serializedData);
                gzipStream.finish();
                gzipStream.flush();
                bodyData = bos.toByteArray();
            } catch (final IOException e) {
                throw new RuntimeException(e);
            }

            final RequestBuilder requestBuilder = new RequestBuilder()
                    .setUri(getAysncHttpClientUri())
                    .setHeader(HttpHeaderNames.CONTENT_TYPE, "application/gzip")
                    .setBody(bodyData)
                    .setMethod(HttpConstants.Methods.POST);

            return new RequestInfo(requestBuilder.build(), serializedData.length, bodyData.length);

        } else {
            return super.createRequest(client, serializedData);
        }
    }

    private void addChunk(
            final ByteArrayOutputStream chunkStream,
            final ByteBuffer currentChunk,
            final Collection<SerializedDatum> completedChunks,
            final long populationSize,
            final LongAdder currentChunkPopulationSize) {
        final byte[] nextChunk = chunkStream.toByteArray();
        final int nextChunkSize = nextChunk.length;
        if (currentChunk.position() + nextChunkSize > _maxRequestSize) {
            if (currentChunk.position() > HEADER_BYTE_LENGTH) {
                // TODO(vkoskela): Add chunk size metric. [MAI-?]

                // Copy the relevant part of the buffer
                currentChunk.put(currentChunk.position() - 1, FOOTER);
                completedChunks.add(new SerializedDatum(
                        Arrays.copyOf(currentChunk.array(), currentChunk.position()),
                        Optional.of(currentChunkPopulationSize.sumThenReset())));

                // Truncate all but the beginning '[' to prepare the next entries
                currentChunk.clear();
                currentChunk.put(HEADER);
            } else {
                CHUNK_TOO_BIG_LOGGER.warn()
                        .setMessage("First chunk too big")
                        .addData("sink", getName())
                        .addData("bufferLength", currentChunk.position())
                        .addData("nextChunkSize", nextChunkSize)
                        .addData("maxRequestSIze", _maxRequestSize)
                        .log();
            }
        }

        currentChunk.put(nextChunk);
        currentChunkPopulationSize.add(populationSize);
        currentChunk.put(SEPARATOR);
        chunkStream.reset();
    }

    private KairosDbSink(final Builder builder) {
        super(builder);
        _maxRequestSize = builder._maxRequestSize;
        _ttlSeconds = (int) builder._ttl.getSeconds();
        _publishStandardMetrics = builder._publishStandardMetrics;
        _publishHistograms = builder._publishHistograms;
        _histogramTtlSeconds = (int) builder._histogramTtl.getSeconds();
    }

    private final int _maxRequestSize;
    private final int _ttlSeconds;
    private final boolean _publishHistograms;
    private final boolean _publishStandardMetrics;
    private final int _histogramTtlSeconds;

    private static final byte HEADER = '[';
    private static final byte FOOTER = ']';
    private static final byte SEPARATOR = ',';
    private static final int HEADER_BYTE_LENGTH = 1;
    // TODO(vkoskela): Switch to ImmutableObjectMapper. [https://github.com/ArpNetworking/commons/issues/7]
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.createInstance();
    private static final Logger LOGGER = LoggerFactory.getLogger(KairosDbSink.class);
    private static final Logger NO_HISTOGRAM_LOGGER = LoggerFactory.getRateLimitLogger(KairosDbSink.class, Duration.ofSeconds(30));
    private static final Logger SERIALIZATION_FAILURE_LOGGER = LoggerFactory.getRateLimitLogger(KairosDbSink.class, Duration.ofSeconds(30));
    private static final Logger CHUNK_TOO_BIG_LOGGER = LoggerFactory.getRateLimitLogger(KairosDbSink.class, Duration.ofSeconds(30));


    private static final class KairosHistogramAdditionalData {
        public double getMin() {
            return _min;
        }

        public void setMin(final double min) {
            _min = min;
        }

        public double getMax() {
            return _max;
        }

        public void setMax(final double max) {
            _max = max;
        }

        public double getMean() {
            return _mean;
        }

        public void setMean(final double mean) {
            _mean = mean;
        }

        public double getSum() {
            return _sum;
        }

        public void setSum(final double sum) {
            _sum = sum;
        }

        private double _min;
        private double _max;
        private double _mean;
        private double _sum;
    }

    private class Serializer {

        Serializer(
                final long timestamp,
                final String serializedPeriod,
                final ImmutableMap<String, String> dimensions) {
            _timestamp = timestamp;
            _serializedPeriod = serializedPeriod;
            _dimensions = dimensions;
        }

        public void serializeDatum(
                final List<SerializedDatum> completeChunks,
                final ByteBuffer currentChunk,
                final ByteArrayOutputStream chunkStream,
                final AggregatedData datum,
                final LongAdder currentChunkPopulationSize) {
            final String name = _serializedPeriod
                    + "/" + datum.getFQDSN().getMetric()
                    + "/" + datum.getFQDSN().getStatistic().getName();
            try {
                final JsonGenerator chunkGenerator = OBJECT_MAPPER.getFactory().createGenerator(chunkStream, JsonEncoding.UTF8);

                chunkGenerator.writeStartObject();
                chunkGenerator.writeStringField("name", name);
                chunkGenerator.writeNumberField("timestamp", _timestamp);
                if (_ttlSeconds > 0) {
                    chunkGenerator.writeNumberField("ttl", _ttlSeconds);
                }
                chunkGenerator.writeNumberField("value", datum.getValue().getValue());
                chunkGenerator.writeObjectFieldStart("tags");
                for (Map.Entry<String, String> entry : _dimensions.entrySet()) {
                    chunkGenerator.writeStringField(entry.getKey(), entry.getValue());
                }
                if (!_dimensions.containsKey("service")) {
                    chunkGenerator.writeStringField("service", datum.getFQDSN().getService());
                }
                if (!_dimensions.containsKey("cluster")) {
                    chunkGenerator.writeStringField("cluster", datum.getFQDSN().getCluster());
                }
                chunkGenerator.writeEndObject();
                chunkGenerator.writeEndObject();

                chunkGenerator.close();

                addChunk(chunkStream, currentChunk, completeChunks, datum.getPopulationSize(), currentChunkPopulationSize);
            } catch (final IOException e) {
                SERIALIZATION_FAILURE_LOGGER.error()
                        .setMessage("Serialization failure")
                        .addData("datum", datum)
                        .setThrowable(e)
                        .log();
            }
        }

        // CHECKSTYLE.OFF: ExecutableStatementCount - There are a lot of fields, we just need to serialize them
        public void serializeHistogram(
                final List<SerializedDatum> completeChunks,
                final ByteBuffer currentChunk,
                final ByteArrayOutputStream chunkStream,
                final AggregatedData data,
                final KairosHistogramAdditionalData additionalData,
                final LongAdder currentChunkPopulationSize) {
            final FQDSN fqdsn = data.getFQDSN();

            try {
                final HistogramStatistic.HistogramSnapshot bins = ((HistogramStatistic.HistogramSupportingData) data.getSupportingData())
                        .getHistogramSnapshot();
                final JsonGenerator chunkGenerator = OBJECT_MAPPER.getFactory().createGenerator(chunkStream, JsonEncoding.UTF8);

                chunkGenerator.writeStartObject();
                chunkGenerator.writeStringField("name", fqdsn.getMetric());
                chunkGenerator.writeNumberField("timestamp", _timestamp);
                chunkGenerator.writeStringField("type", "histogram");

                chunkGenerator.writeObjectFieldStart("value");
                chunkGenerator.writeNumberField("min", additionalData.getMin());
                chunkGenerator.writeNumberField("max", additionalData.getMax());
                chunkGenerator.writeNumberField("mean", additionalData.getMean());
                chunkGenerator.writeNumberField("sum", additionalData.getSum());
                chunkGenerator.writeNumberField("precision", bins.getPrecision());
                chunkGenerator.writeObjectFieldStart("bins");
                for (Double2LongMap.Entry bin : bins.getValues()) {
                    chunkGenerator.writeNumberField(String.valueOf(bin.getDoubleKey()), bin.getLongValue());
                }

                chunkGenerator.writeEndObject();  //close bins
                chunkGenerator.writeEndObject();  //close value

                if (_histogramTtlSeconds > 0) {
                    chunkGenerator.writeNumberField("ttl", _histogramTtlSeconds);
                }
                chunkGenerator.writeObjectFieldStart("tags");
                for (Map.Entry<String, String> entry : _dimensions.entrySet()) {
                    chunkGenerator.writeStringField(entry.getKey(), entry.getValue());
                }
                if (!_dimensions.containsKey("service")) {
                    chunkGenerator.writeStringField("service", fqdsn.getService());
                }
                if (!_dimensions.containsKey("cluster")) {
                    chunkGenerator.writeStringField("cluster", fqdsn.getCluster());
                }
                chunkGenerator.writeEndObject();
                chunkGenerator.writeEndObject();

                chunkGenerator.close();

                addChunk(chunkStream, currentChunk, completeChunks, data.getPopulationSize(), currentChunkPopulationSize);
            } catch (final IOException e) {
                SERIALIZATION_FAILURE_LOGGER.error()
                        .setMessage("Serialization failure")
                        .addData("data", data)
                        .setThrowable(e)
                        .log();
            }
        }
        // CHECKSTYLE.ON: ExecutableStatementCount

        public void serializeCondition(
                final List<SerializedDatum> completeChunks,
                final ByteBuffer currentChunk,
                final ByteArrayOutputStream chunkStream,
                final Condition condition,
                final LongAdder currentChunkPopulationSize) {
            final String conditionName = _serializedPeriod
                    + "/" + condition.getFQDSN().getMetric()
                    + "/" + condition.getFQDSN().getStatistic().getName()
                    + "/" + condition.getName();
            final String conditionStatusName = conditionName
                    + "/status";
            try {
                // Value for condition threshold
                serializeConditionThreshold(
                        completeChunks,
                        currentChunk,
                        chunkStream,
                        condition,
                        conditionName,
                        currentChunkPopulationSize);

                if (condition.isTriggered().isPresent()) {
                    // Value for condition trigger (or status)
                    serializeConditionStatus(
                            completeChunks,
                            currentChunk,
                            chunkStream,
                            condition,
                            conditionStatusName,
                            currentChunkPopulationSize);

                }
            } catch (final IOException e) {
                SERIALIZATION_FAILURE_LOGGER.error()
                        .setMessage("Serialization failure")
                        .addData("condition", condition)
                        .setThrowable(e)
                        .log();
            }
        }

        private void serializeConditionStatus(
                final List<SerializedDatum> completeChunks,
                final ByteBuffer currentChunk,
                final ByteArrayOutputStream chunkStream,
                final Condition condition,
                final String conditionStatusName,
                final LongAdder currentChunkPopulationSize)
                throws IOException {
            // 0 = Not triggered
            // 1 = Triggered
            final JsonGenerator chunkGenerator = OBJECT_MAPPER.getFactory().createGenerator(chunkStream, JsonEncoding.UTF8);

            chunkGenerator.writeStartObject();
            chunkGenerator.writeStringField("name", conditionStatusName);
            chunkGenerator.writeNumberField("timestamp", _timestamp);
            if (_ttlSeconds > 0) {
                chunkGenerator.writeNumberField("ttl", _ttlSeconds);
            }
            chunkGenerator.writeNumberField("value", condition.isTriggered().get() ? 1 : 0);
            chunkGenerator.writeObjectFieldStart("tags");
            for (Map.Entry<String, String> entry : _dimensions.entrySet()) {
                chunkGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
            if (!_dimensions.containsKey("service")) {
                chunkGenerator.writeStringField("service", condition.getFQDSN().getService());
            }
            if (!_dimensions.containsKey("cluster")) {
                chunkGenerator.writeStringField("cluster", condition.getFQDSN().getCluster());
            }
            chunkGenerator.writeEndObject();
            chunkGenerator.writeEndObject();

            chunkGenerator.close();

            addChunk(chunkStream, currentChunk, completeChunks, 0L, currentChunkPopulationSize);
        }

        private void serializeConditionThreshold(
                final List<SerializedDatum> completeChunks,
                final ByteBuffer currentChunk,
                final ByteArrayOutputStream chunkStream,
                final Condition condition,
                final String conditionName,
                final LongAdder currentChunkPopulationSize)
                throws IOException {
            final JsonGenerator chunkGenerator = OBJECT_MAPPER.getFactory().createGenerator(chunkStream, JsonEncoding.UTF8);

            chunkGenerator.writeStartObject();
            chunkGenerator.writeStringField("name", conditionName);
            chunkGenerator.writeNumberField("timestamp", _timestamp);
            chunkGenerator.writeNumberField("value", condition.getThreshold().getValue());
            chunkGenerator.writeObjectFieldStart("tags");
            for (Map.Entry<String, String> entry : _dimensions.entrySet()) {
                chunkGenerator.writeStringField(entry.getKey(), entry.getValue());
            }
            if (!_dimensions.containsKey("service")) {
                chunkGenerator.writeStringField("service", condition.getFQDSN().getService());
            }
            if (!_dimensions.containsKey("cluster")) {
                chunkGenerator.writeStringField("cluster", condition.getFQDSN().getCluster());
            }
            chunkGenerator.writeEndObject();
            chunkGenerator.writeEndObject();

            chunkGenerator.close();

            addChunk(chunkStream, currentChunk, completeChunks, 0L, currentChunkPopulationSize);
        }

        private final long _timestamp;
        private final String _serializedPeriod;
        private final ImmutableMap<String, String> _dimensions;
    }

    /**
     * Implementation of builder pattern for {@link KairosDbSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static class Builder extends HttpPostSink.Builder<Builder, KairosDbSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(KairosDbSink::new);
        }

        @Override
        protected Builder self() {
            return this;
        }

        /**
         * Sets the maximum size of the request to publish.
         * Optional. Defaults to 100KiB.
         *
         * @param value the maximum request size.
         * @return This instance of {@link Builder}.
         */
        public Builder setMaxRequestSize(final Integer value) {
            _maxRequestSize = value;
            return this;
        }

        /**
         * Sets whether or not to publish non-histogram metrics.
         * Optional.  Defaults to true.
         *
         * @param value true to publish standard metrics
         * @return This instance of {@link Builder}.
         */
        public Builder setPublishStandardMetrics(final Boolean value) {
            _publishStandardMetrics = value;
            return this;
        }

        /**
         * Sets whether or not to publish full histograms.
         * Optional.  Defaults to false.
         *
         * @param value true to publish histograms
         * @return This instance of {@link Builder}.
         */
        public Builder setPublishHistograms(final Boolean value) {
            _publishHistograms = value;
            return this;
        }

        /**
         * Sets the TTL of non-histogram metrics.
         * NOTE: A value of 0 represents permanent.
         * Optional.  Defaults to permanent.
         *
         * @param value the time to retain histograms
         * @return This instance of {@link Builder}.
         */
        public Builder setTtl(final Duration value) {
            _ttl = value;
            return this;
        }

        /**
         * Sets the TTL of histograms.
         * NOTE: A value of 0 represents permanent.
         * Optional.  Defaults to permanent.
         *
         * @param value the time to retain histograms
         * @return This instance of {@link Builder}.
         */
        public Builder setHistogramTtl(final Duration value) {
            _histogramTtl = value;
            return this;
        }

        @NotNull
        @Min(value = 0)
        private Integer _maxRequestSize = 100 * 1024;
        @NotNull
        private Boolean _publishStandardMetrics = true;
        @NotNull
        private Boolean _publishHistograms = false;
        @NotNull
        private Duration _histogramTtl = Duration.ofSeconds(0);
        @NotNull
        private Duration _ttl = Duration.ofSeconds(0);
    }
}
