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

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.ReceiveTimeout;
import org.apache.pekko.cluster.sharding.ShardRegion;

import java.io.Serializable;
import java.time.Duration;
import java.util.Optional;

/**
 * Actual actor responsible for aggregating.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class AggregationRouter extends AbstractActor {

    /**
     * Creates a {@link Props} for use in Pekko.
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
                AggregationRouter.class,
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
    public AggregationRouter(
            @Named("periodic-statistics") final ActorRef periodicStatistics,
            @Named("cluster-emitter") final ActorRef emitter,
            @Named("cluster-host-suffix") final String clusterHostSuffix,
            @Named("reaggregation-dimensions") final ImmutableSet<String> reaggregationDimensions,
            @Named("reaggregation-cluster-as-host") final boolean injectClusterAsHost,
            @Named("reaggregation-timeout") final Duration aggregatorTimeout,
            final PeriodicMetrics periodicMetrics) {
        _periodicMetrics = periodicMetrics;
        _streamingChild = context().actorOf(
                StreamingAggregator.props(
                        periodicStatistics,
                        emitter,
                        clusterHostSuffix,
                        reaggregationDimensions,
                        injectClusterAsHost,
                        aggregatorTimeout,
                        periodicMetrics),
                "streaming");
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Messages.StatisticSetRecord.class, message -> {
                    _streamingChild.forward(message, context());
                })
                .match(ShutdownAggregator.class, message -> {
                    // TODO(barp): review the implications of shutting down (do the children process all of the messages properly?) [AINT-?]
                    _streamingChild.forward(message, context());
                    context().stop(self());
                })
                .match(ReceiveTimeout.class, message -> {
                    getContext().parent().tell(new ShardRegion.Passivate(new ShutdownAggregator()), getSelf());
                })
                .build();
    }

    @Override
    public void preStart() {
        _periodicMetrics.recordCounter("actors/aggregation_router/started", 1);
    }

    @Override
    public void postStop() {
        _periodicMetrics.recordCounter("actors/aggregation_router/stopped", 1);
    }

    @Override
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
        _periodicMetrics.recordCounter("actors/aggregation_router/restarted", 1);
        LOGGER.error()
                .setMessage("Aggregator crashing")
                .setThrowable(reason)
                .addData("triggeringMessage", message)
                .addContext("actor", self())
                .log();
        super.preRestart(reason, message);
    }

    private final ActorRef _streamingChild;
    private final PeriodicMetrics _periodicMetrics;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationRouter.class);

    /**
     * Message to request shutdown of aggregation router and subordinate
     * streaming aggregator actor pair.
     */
    public static final class ShutdownAggregator implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
