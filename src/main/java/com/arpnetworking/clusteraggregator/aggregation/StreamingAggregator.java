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

import akka.actor.AbstractActorWithTimers;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.cluster.sharding.ShardRegion;
import com.arpnetworking.clusteraggregator.models.CombinedMetricData;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
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
import com.google.common.collect.Sets;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.LinkedList;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
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
     * @param livelinessTimeout How often to check that this actor is still receiving data.
     * @return A new {@link Props}.
     */
    public static Props props(
            final ActorRef metricsListener,
            final ActorRef emitter,
            final String clusterHostSuffix,
            final ImmutableSet<String> reaggregationDimensions,
            final boolean injectClusterAsHost,
            final Duration aggregatorTimeout,
            final Duration livelinessTimeout) {
        return Props.create(
                StreamingAggregator.class,
                metricsListener,
                emitter,
                clusterHostSuffix,
                reaggregationDimensions,
                injectClusterAsHost,
                aggregatorTimeout,
                livelinessTimeout);
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
     * @param livelinessTimeout How often to check that this actor is still receiving data.
     */
    @Inject
    public StreamingAggregator(
            @Named("periodic-statistics") final ActorRef periodicStatistics,
            @Named("cluster-emitter") final ActorRef emitter,
            @Named("cluster-host-suffix") final String clusterHostSuffix,
            @Named("reaggregation-dimensions") final ImmutableSet<String> reaggregationDimensions,
            @Named("reaggregation-cluster-as-host") final boolean injectClusterAsHost,
            @Named("reaggregation-timeout") final Duration aggregatorTimeout,
            @Named("aggregator-liveliness-timeout") final Duration livelinessTimeout) {
        _periodicStatistics = periodicStatistics;
        _clusterHostSuffix = clusterHostSuffix;
        _reaggregationDimensions = reaggregationDimensions;
        _injectClusterAsHost = injectClusterAsHost;
        _aggregatorTimeout = aggregatorTimeout;
        context().setReceiveTimeout(FiniteDuration.apply(30, TimeUnit.MINUTES));

        timers().startPeriodicTimer(BUCKET_CHECK_TIMER_KEY, BucketCheck.getInstance(), FiniteDuration.apply(5, TimeUnit.SECONDS));
        timers().startPeriodicTimer(LIVELINESS_CHECK_TIMER, LIVELINESS_CHECK_MSG, livelinessTimeout);

        _emitter = emitter;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.StatisticSetRecord.class, record -> {
                    // Mark this actor as live since we're still receiving data.
                    _live = true;
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
                                    _statistics.add(entry.getKey());
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
                                _emitter.tell(periodicData, getSelf());
                            } else {
                                // Walk of the list is complete
                                break;
                            }
                        }
                    }
                })
                .matchEquals(LIVELINESS_CHECK_MSG, msg -> {
                    // If we've received data since our last check, reset and wait until another round.
                    // otherwise shutdown.
                    if (_live) {
                        LOGGER.debug()
                                .setMessage("aggregator is still live, continuing.")
                                .addContext("actor", self())
                                .log();
                        _live = false;
                        return;
                    }
                    LOGGER.debug()
                            .setMessage("aggregator is stale, requesting shutdown.")
                            .addContext("actor", self())
                            .log();
                    requestShutdownFromParent();
                })
                .match(ShutdownAggregator.class, message -> context().stop(self()))
                .match(ReceiveTimeout.class, message -> requestShutdownFromParent())
                .build();
    }

    @Override
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
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

        // First message sets the data we know about this actor
        initialize(data, metricData);

        // Find the time bucket to dump this in
        final ZonedDateTime periodStart = ZonedDateTime.parse(data.getPeriodStart());
        if (_aggBuckets.size() > 0 && _aggBuckets.getFirst().getPeriodStart().isAfter(periodStart)) {
            // We got a bit of data that is too old for us to aggregate.
            WORK_TOO_OLD_LOGGER.warn()
                    .setMessage("Received a work item that is too old to aggregate")
                    .addData("start", periodStart)
                    .addData("firstStart", _aggBuckets.getFirst().getPeriodStart())
                    .addData("metric", data.getMetric())
                    .addData("dimensions", data.getDimensionsMap())
                    .addContext("actor", self())
                    .log();
        } else {
            // TODO(ville): Support skipped bucket creation.
            StreamingAggregationBucket correctBucket = null;
            if (_aggBuckets.size() == 0 || _aggBuckets.getLast().getPeriodStart().isBefore(periodStart)) {
                // We need to create a new bucket to hold this data.
                if (_aggBuckets.size() != 0 && !_aggBuckets.getLast().getPeriodStart().equals(periodStart.minus(_period))) {
                    NON_CONSECUTIVE_LOGGER.warn()
                            .setMessage("Creating new non-consecutive aggregation bucket for period")
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
                            .addContext("actor", self())
                            .log();
                }
                correctBucket = new StreamingAggregationBucket(periodStart);
                _aggBuckets.add(correctBucket);
            } else {
                for (final StreamingAggregationBucket currentBucket : _aggBuckets) {
                    if (currentBucket.getPeriodStart().equals(periodStart)) {
                        // We found the correct bucket
                        correctBucket = currentBucket;
                        break;
                    }
                }
            }
            if (correctBucket == null) {
                LOGGER.error()
                        .setMessage("No bucket found to aggregate into, bug in the bucket walk")
                        .addData("start", periodStart)
                        .addData("firstStart", _aggBuckets.getFirst().getPeriodStart())
                        .addData("lastStart", _aggBuckets.getLast().getPeriodStart())
                        .addData("metric", data.getMetric())
                        .addData("dimensions", data.getDimensionsMap())
                        .addContext("actor", self())
                        .log();
            } else {
                LOGGER.debug()
                        .setMessage("Updating bucket")
                        .addData("bucket", correctBucket)
                        .addData("data", metricData)
                        .addContext("actor", self())
                        .log();
                correctBucket.update(metricData);
            }
        }
    }

    private void initialize(final Messages.StatisticSetRecord data, final CombinedMetricData metricData) {
        if (!_initialized) {
            _period = metricData.getPeriod();
            _cluster = metricData.getCluster();
            _metric = metricData.getMetricName();
            _service = metricData.getService();
            _dimensions = dimensionsToMap(data);
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
                && _dimensions.equals(dimensionsToMap(data)))) {
            LOGGER.error()
                    .setMessage("Received a work item for another aggregator")
                    .addData("data", metricData)
                    .addData("actorPeriod", _period)
                    .addData("actorCluster", _cluster)
                    .addData("actorService", _service)
                    .addData("actorMetric", _metric)
                    .addData("actorDimensions", _dimensions)
                    .addContext("actor", self())
                    .log();
        }
    }

    private void requestShutdownFromParent() {
        getContext().parent().tell(new ShardRegion.Passivate(ShutdownAggregator.getInstance()), getSelf());
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

    private final LinkedList<StreamingAggregationBucket> _aggBuckets = Lists.newLinkedList();
    private final ActorRef _emitter;
    private final ActorRef _periodicStatistics;
    private final String _clusterHostSuffix;
    private final ImmutableSet<String> _reaggregationDimensions;
    private final boolean _injectClusterAsHost;
    private final Set<Statistic> _statistics = Sets.newHashSet();
    private final Duration _aggregatorTimeout;
    private boolean _initialized = false;
    // This actor is _live if it's received data since the last LIVELINESS_CHECK_MSG.
    // If this is ever false during a check, the actor will shutdown.
    private boolean _live = false;
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
    private static final String LIVELINESS_CHECK_TIMER = "livelinesscheck";

    private static final String LIVELINESS_CHECK_MSG = "LIVELINESS_CHECK_MSG";

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
