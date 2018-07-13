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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.ReceiveTimeout;
import akka.cluster.sharding.ShardRegion;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.collect.ImmutableSet;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Actual actor responsible for aggregating.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class AggregationRouter extends AbstractActor {

    /**
     * Creates a <code>Props</code> for use in Akka.
     *
     * @param lifecycleTracker Where to register the liveliness of this aggregator.
     * @param metricsListener Where to send metrics about aggregation computations.
     * @param emitter Where to send the metrics data.
     * @param clusterHostSuffix The suffix to append to the hostname for cluster aggregations.
     * @param reaggregationDimensions The dimensions to reaggregate over.
     * @param injectClusterAsHost Whether to inject a host dimension based on cluster.
     * @return A new <code>Props</code>.
     */
    public static Props props(
            final ActorRef lifecycleTracker,
            final ActorRef metricsListener,
            final ActorRef emitter,
            final String clusterHostSuffix,
            final ImmutableSet<String> reaggregationDimensions,
            final boolean injectClusterAsHost) {
        return Props.create(
                AggregationRouter.class,
                lifecycleTracker,
                metricsListener,
                emitter,
                clusterHostSuffix,
                reaggregationDimensions,
                injectClusterAsHost);
    }

    /**
     * Public constructor.
     *
     * @param lifecycleTracker Where to register the liveliness of this aggregator.
     * @param periodicStatistics Where to send metrics about aggregation computations.
     * @param emitter Where to send the metrics data.
     * @param clusterHostSuffix The suffix to append to the hostname for cluster aggregations.
     * @param reaggregationDimensions The dimensions to reaggregate over.
     * @param injectClusterAsHost Whether to inject a host dimension based on cluster.
     */
    @Inject
    public AggregationRouter(
            @Named("bookkeeper-proxy") final ActorRef lifecycleTracker,
            @Named("periodic-statistics") final ActorRef periodicStatistics,
            @Named("cluster-emitter") final ActorRef emitter,
            @Named("cluster-host-suffix") final String clusterHostSuffix,
            @Named("reaggregation-dimensions") final ImmutableSet<String> reaggregationDimensions,
            @Named("reaggregation-cluster-as-host") final boolean injectClusterAsHost) {
        _streamingChild = context().actorOf(
                StreamingAggregator.props(
                        lifecycleTracker,
                        periodicStatistics,
                        emitter,
                        clusterHostSuffix,
                        reaggregationDimensions,
                        injectClusterAsHost),
                "streaming");
        context().setReceiveTimeout(FiniteDuration.apply(30, TimeUnit.MINUTES));
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
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
        LOGGER.error()
                .setMessage("Aggregator crashing")
                .setThrowable(reason)
                .addData("triggeringMessage", message)
                .addContext("actor", self())
                .log();
        super.preRestart(reason, message);
    }

    private final ActorRef _streamingChild;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationRouter.class);

    private static final class ShutdownAggregator implements Serializable {
        private static final long serialVersionUID = 1L;
    }
}
