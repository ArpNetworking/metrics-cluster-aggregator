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
package com.arpnetworking.clusteraggregator.aggregation;

import com.arpnetworking.clusteraggregator.models.CombinedMetricData;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.AbstractActorWithTimers;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import javax.annotation.Nullable;

/**
 * Actual actor responsible for aggregating.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class StreamingAggregator extends AbstractActorWithTimers {

    /**
     * Creates a {@link Props} for use in Akka.
     *
     * @param metricsListener Where to send metrics about aggregation computations.
     * @param emitter Where to send the metrics data.
     * @param clusterHostSuffix The suffix to append to the hostname for cluster aggregations.
     * @param reaggregationDimensions The dimensions to reaggregate over.
     * @param injectClusterAsHost Whether to inject a host dimension based on cluster.
     * @param aggregatorTimeout The time to wait from the start of the period for all data.
     * @param periodicMetrics The {@link PeriodicMetrics} instance.
     * @return A new {@link Props}.
     */
    public static Props props(
            final ActorRef metricsListener,
            final ActorRef emitter,
            final String clusterHostSuffix,
            final ImmutableSet<String> reaggregationDimensions,
            final boolean injectClusterAsHost,
            final Duration aggregatorTimeout,
            final PeriodicMetrics periodicMetrics) {
        return Props.create(
                StreamingAggregator.class,
                metricsListener,
                emitter,
                clusterHostSuffix,
                reaggregationDimensions,
                injectClusterAsHost,
                aggregatorTimeout,
                periodicMetrics);
    }

    /**
     * Public constructor.
     *
     * @param periodicStatistics Where to send metrics about aggregation computations.
     * @param emitter Where to send the metrics data.
     * @param clusterHostSuffix The suffix to append to the hostname for cluster aggregations.
     * @param reaggregationDimensions The dimensions to reaggregate over.
     * @param injectClusterAsHost Whether to inject a host dimension based on cluster.
     * @param aggregatorTimeout The time to wait from the start of the period for all data.
     * @param periodicMetrics The {@link PeriodicMetrics} instance.
     */
    @Inject
    @SuppressFBWarnings(value = "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR", justification = "context is safe to be used in constructors")
    public StreamingAggregator(
            @Named("periodic-statistics") final ActorRef periodicStatistics,
            @Named("cluster-emitter") final ActorRef emitter,
            @Named("cluster-host-suffix") final String clusterHostSuffix,
            @Named("reaggregation-dimensions") final ImmutableSet<String> reaggregationDimensions,
            @Named("reaggregation-cluster-as-host") final boolean injectClusterAsHost,
            @Named("reaggregation-timeout") final Duration aggregatorTimeout,
            final PeriodicMetrics periodicMetrics) {
        _periodicStatistics = periodicStatistics;
        _clusterHostSuffix = clusterHostSuffix;
        _reaggregationDimensions = reaggregationDimensions;
        _injectClusterAsHost = injectClusterAsHost;
        _aggregatorTimeout = aggregatorTimeout;
        _periodicMetrics = periodicMetrics;
        context().setReceiveTimeout(FiniteDuration.apply(30, TimeUnit.MINUTES));

        timers().startTimerAtFixedRate(BUCKET_CHECK_TIMER_KEY, BucketCheck.getInstance(), FiniteDuration.apply(5, TimeUnit.SECONDS));

        _emitter = emitter;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.StatisticSetRecord.class, record -> {
                    LOGGER.debug()
                            .setMessage("Processing a StatisticSetRecord")
                            .addData("workItem", record)
                            .addContext("actor", self())
                            .log();
                    processAggregationMessage(record);
                })
                .match(BucketCheck.class, message -> {
                    if (_initialized) {
                        while (!_aggBuckets.isEmpty()) {
                            final StreamingAggregationBucket bucket = _aggBuckets.getFirst();
                            if (bucket.getPeriodStart().plus(_period).plus(_aggregatorTimeout).isBefore(ZonedDateTime.now())) {
                                _aggBuckets.removeFirst();

                                // Walk over every statistic in the bucket
                                final Map<Statistic, CalculatedValue<?>> values = bucket.compute();
                                final long populationSize = CombinedMetricData.computePopulationSizeFromCalculatedValues(
                                            _metric,
                                            values);
                                final ImmutableList.Builder<AggregatedData> builder = ImmutableList.builder();
                                for (final Map.Entry<Statistic, CalculatedValue<?>> entry : values.entrySet()) {
                                    final AggregatedData result = _resultBuilder
                                            .setFQDSN(
                                                    new FQDSN.Builder()
                                                            .setCluster(_cluster)
                                                            .setMetric(_metric)
                                                            .setService(_service)
                                                            .setStatistic(entry.getKey())
                                                            .build())
                                            .setStart(bucket.getPeriodStart())
                                            .setValue(entry.getValue().getValue())
                                            .setSupportingData(entry.getValue().getData())
                                            .setPopulationSize(populationSize)
                                            .setIsSpecified(bucket.isSpecified(entry.getKey()))
                                            .build();
                                    LOGGER.debug()
                                            .setMessage("Computed result")
                                            .addData("result", result)
                                            .addContext("actor", self())
                                            .log();
                                    builder.add(result);

                                    _periodicStatistics.tell(result, getSelf());
                                }
                                final PeriodicData periodicData = new PeriodicData.Builder()
                                        .setData(builder.build())
                                        .setDimensions(_dimensions)
                                        .setConditions(ImmutableList.of())
                                        .setPeriod(_period)
                                        .setStart(bucket.getPeriodStart())
                                        .setMinRequestTime(bucket.getMinRequestTime().orElse(null))
                                        .build();
                                // This cannot be accumulated in an AtomicLong and then periodically flushed
                                // because the StreamingAggregator instance is ephemeral and there is currently
                                // no way to deregister the callback.
                                _periodicMetrics.recordGauge(
                                        "actors/streaming_aggregator/metric_samples/out",
                                        populationSize);
                                _emitter.tell(periodicData, getSelf());
                            } else {
                                // Walk of the list is complete
                                break;
                            }
                        }
                    }
                })
                .match(ShutdownAggregator.class, message -> context().stop(self()))
                .build();
    }

    @Override
    public void preStart() {
        _periodicMetrics.recordCounter("actors/streaming_aggregator/started", 1);
    }

    @Override
    public void postStop() {
        _periodicMetrics.recordCounter("actors/streaming_aggregator/stopped", 1);
    }

    @Override
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
        _periodicMetrics.recordCounter("actors/streaming_aggregator/restarted", 1);
        LOGGER.error()
                .setMessage("Aggregator crashing")
                .setThrowable(reason)
                .addData("triggeringMessage", message.orElse(null))
                .addContext("actor", self())
                .log();
        super.preRestart(reason, message);
    }

    private void processAggregationMessage(final Messages.StatisticSetRecord data) {
        final CombinedMetricData metricData = CombinedMetricData.Builder.fromStatisticSetRecord(data).build();

        final double samples = data.getStatisticsList().stream()
                .filter(s -> s.getStatistic().equals("count"))
                .map(Messages.StatisticRecord::getValue)
                .reduce(Double::sum)
                .orElse(0.0d);

        // Record samples received
        _periodicMetrics.recordGauge(
                "actors/streaming_aggregator/metric_samples/in",
                samples);

        // First message sets the data we know about this actor
        initialize(data, metricData);

        // Find the time bucket to dump this in
        final ZonedDateTime periodStart = ZonedDateTime.parse(data.getPeriodStart());
        if (periodStart.plus(_period).plus(_aggregatorTimeout).isBefore(ZonedDateTime.now())) {
            // We got a bit of data that is too old for us to aggregate.
            @Nullable final StreamingAggregationBucket firstBucket = _aggBuckets.getFirst();
            WORK_TOO_OLD_LOGGER.warn()
                    .setMessage("Received a work item that is too old to aggregate")
                    .addData("start", periodStart)
                    .addData("firstStart", firstBucket == null ? null : firstBucket.getPeriodStart())
                    .addData("metric", data.getMetric())
                    .addData("dimensions", data.getDimensionsMap())
                    .addContext("actor", self())
                    .log();
            _periodicMetrics.recordGauge("actors/streaming_aggregator/metric_samples/too_old", samples);
        } else {
            // Locate the matching bucket or if it does not exist then locate the index where it should
            @Nullable StreamingAggregationBucket correctBucket = null;
            int targetIndex = 0;
            for (final StreamingAggregationBucket bucket : _aggBuckets) {
                if (bucket.getPeriodStart().isAfter(periodStart)) {
                    // Correct bucket is before the current bucket
                    break;
                }
                if (bucket.getPeriodStart().equals(periodStart)) {
                    // Found the correct bucket
                    correctBucket = bucket;
                    break;
                }
                ++targetIndex;
            }

            // Create the bucket at the right index if a matching one does not already exist
            if (correctBucket == null) {
                if (!_aggBuckets.isEmpty() && targetIndex != _aggBuckets.size()) {
                    NON_CONSECUTIVE_LOGGER.warn()
                            .setMessage("Creating new out of order aggregation bucket for period")
                            .addData("start", periodStart)
                            .addData("lastStart", _aggBuckets.getLast().getPeriodStart())
                            .addData("metric", data.getMetric())
                            .addData("dimensions", data.getDimensionsMap())
                            .addContext("actor", self())
                            .log();
                } else {
                    LOGGER.debug()
                            .setMessage("Creating new aggregation bucket for period")
                            .addData("start", periodStart)
                            .addData("metric", data.getMetric())
                            .addData("dimensions", data.getDimensionsMap())
                            .addContext("actor", self())
                            .log();
                }

                correctBucket = new StreamingAggregationBucket(periodStart);
                _aggBuckets.add(targetIndex, correctBucket);
            }

            LOGGER.debug()
                    .setMessage("Updating bucket")
                    .addData("bucket", correctBucket)
                    .addData("data", metricData)
                    .addContext("actor", self())
                    .log();
            correctBucket.update(metricData);
        }
    }

    private void initialize(final Messages.StatisticSetRecord data, final CombinedMetricData metricData) {
        final ImmutableMap<String, String> dataDimensions = dimensionsToMap(data);
        if (!_initialized) {
            _period = metricData.getPeriod();
            _cluster = metricData.getCluster();
            _metric = metricData.getMetricName();
            _service = metricData.getService();
            _dimensions = dataDimensions;
            _resultBuilder = new AggregatedData.Builder()
                    .setHost(createHost())
                    .setPeriod(_period)
                    .setPopulationSize(1L)
                    .setSamples(Collections.emptyList())
                    .setStart(ZonedDateTime.now().truncatedTo(ChronoUnit.HOURS))
                    .setValue(new Quantity.Builder().setValue(0d).build());
            _initialized = true;
            LOGGER.debug()
                    .setMessage("Initialized aggregator")
                    .addContext("actor", self())
                    .log();
        } else if (!(_period.equals(metricData.getPeriod())
                && _cluster.equals(metricData.getCluster())
                && _service.equals(metricData.getService())
                && _metric.equals(metricData.getMetricName())
                && _dimensions.equals(dataDimensions))) {
            LOGGER.error()
                    .setMessage("Received a work item for another aggregator")
                    .addData("data", metricData)
                    .addData("data", dataDimensions)
                    .addData("actorPeriod", _period)
                    .addData("actorCluster", _cluster)
                    .addData("actorService", _service)
                    .addData("actorMetric", _metric)
                    .addData("actorDimensions", _dimensions)
                    .addContext("actor", self())
                    .log();
        }
    }

    private ImmutableMap<String, String> dimensionsToMap(final Messages.StatisticSetRecord statisticSetRecord) {
        // Build a map of dimension key-value pairs dropping any that are to be reaggregated over
        final ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();

        // Grab the explicit cluster and service dimensions from the record
        addDimension(CombinedMetricData.CLUSTER_KEY, statisticSetRecord.getCluster(), builder);
        addDimension(CombinedMetricData.SERVICE_KEY, statisticSetRecord.getService(), builder);

        // Either inject the cluster as the host dimension or grab it from the record dimensions
        if (_injectClusterAsHost) {
            addDimension(CombinedMetricData.HOST_KEY, createHost(), builder);
        } else {
            @Nullable final String hostDimension = statisticSetRecord.getDimensionsMap().get(CombinedMetricData.HOST_KEY);
            if (hostDimension != null) {
                addDimension(CombinedMetricData.HOST_KEY, hostDimension, builder);
            }
        }

        // Inject all other dimensions (e.g. not service, cluster or host)
        statisticSetRecord.getDimensionsMap()
                .entrySet()
                .stream()
                .filter(NOT_EXPLICIT_DIMENSION)
                .forEach(entry -> addDimension(entry.getKey(), entry.getValue(), builder));

        return builder.build();
    }

    private void addDimension(final String key, final String value, final ImmutableMap.Builder<String, String> mapBuilder) {
        if (!_reaggregationDimensions.contains(key)) {
            mapBuilder.put(key, value);
        }
    }

    private String createHost() {
        return _cluster + "-cluster" + _clusterHostSuffix;
    }

    // WARNING: Consider carefully the volume of samples recorded.
    // PeriodicMetrics reduces the number of scopes creates, but each sample is
    // still stored in-memory until it is flushed.
    private final PeriodicMetrics _periodicMetrics;

    private final LinkedList<StreamingAggregationBucket> _aggBuckets = Lists.newLinkedList();
    private final ActorRef _emitter;
    private final ActorRef _periodicStatistics;
    private final String _clusterHostSuffix;
    private final ImmutableSet<String> _reaggregationDimensions;
    private final boolean _injectClusterAsHost;
    private final Duration _aggregatorTimeout;
    private boolean _initialized = false;
    private Duration _period;
    private String _cluster;
    private String _metric;
    private String _service;
    private ImmutableMap<String, String> _dimensions;
    private AggregatedData.Builder _resultBuilder;
    private static final Predicate<Map.Entry<String, String>> NOT_EXPLICIT_DIMENSION = entry ->
            !(entry.getKey().equals(CombinedMetricData.CLUSTER_KEY)
                    || entry.getKey().equals(CombinedMetricData.HOST_KEY)
                    || entry.getKey().equals(CombinedMetricData.SERVICE_KEY));
    private static final String BUCKET_CHECK_TIMER_KEY = "bucketcheck";

    private static final Logger LOGGER = LoggerFactory.getLogger(StreamingAggregator.class);
    private static final Logger WORK_TOO_OLD_LOGGER = LoggerFactory.getRateLimitLogger(
            StreamingAggregator.class, Duration.ofSeconds(30));
    private static final Logger NON_CONSECUTIVE_LOGGER = LoggerFactory.getRateLimitLogger(
            StreamingAggregator.class, Duration.ofSeconds(30));


    private static final class BucketCheck implements Serializable {
        /**
         * Gets the singleton instance.
         *
         * @return singleton instance
         */
        public static BucketCheck getInstance() {
            return INSTANCE;
        }

        private static final BucketCheck INSTANCE = new BucketCheck();
        private static final long serialVersionUID = 1L;
    }

    private static final class ShutdownAggregator implements Serializable {
        /**
         * Gets the singleton instance.
         *
         * @return singleton instance
         */
        public static ShutdownAggregator getInstance() {
            return INSTANCE;
        }

        private static final ShutdownAggregator INSTANCE = new ShutdownAggregator();
        private static final long serialVersionUID = 1L;
    }
}
