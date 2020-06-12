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

package com.arpnetworking.clusteraggregator;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.MemberStatus;
import akka.pattern.PatternsCS;
import akka.remote.AssociationErrorEvent;
import com.arpnetworking.clusteraggregator.models.MetricsRequest;
import com.arpnetworking.clusteraggregator.models.PeriodMetrics;
import com.arpnetworking.clusteraggregator.models.StatusResponse;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.utility.CastMapper;
import org.joda.time.Period;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

/**
 * Periodically polls the cluster status and caches the result.
 *
 * Accepts the following messages:
 *     {@link StatusRequest}: Replies with a StatusResponse message containing the service status data
 *     {@link HealthRequest}: Replies with a boolean value, true indicating healthy, false indicating unhealthy
 *
 * Internal-only messages:
 *     {@link AssociationErrorEvent}: Evaluates the possibility of the node being quarantined
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class Status extends AbstractActor {
    /**
     * Public constructor.
     *
     * @param cluster The instance of the Clustering extension.
     * @param clusterStatusCache The actor holding the cached cluster status.
     * @param localMetrics The actor holding the local node metrics.
     */
    public Status(
            final Cluster cluster,
            final ActorRef clusterStatusCache,
            final ActorRef localMetrics) {

        _cluster = cluster;
        _clusterStatusCache = clusterStatusCache;
        _localMetrics = localMetrics;
        context().system().eventStream().subscribe(self(), AssociationErrorEvent.class);
    }

    /**
     * Creates a {@link Props} for use in Akka.
     *
     * @param cluster The instance of the Clustering extension.
     * @param clusterStatusCache The actor holding the cached cluster status.
     * @param localMetrics The actor holding the local node metrics.
     * @return A new {@link Props}.
     */
    public static Props props(
            final Cluster cluster,
            final ActorRef clusterStatusCache,
            final ActorRef localMetrics) {

        return Props.create(Status.class, cluster, clusterStatusCache, localMetrics);
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(StatusRequest.class, message -> processStatusRequest())
                .match(AssociationErrorEvent.class, error -> {
                    if (error.cause().getMessage().contains("quarantined this system")) {
                        _quarantined = true;
                        LOGGER.error()
                                .setMessage("This node is quarantined.")
                                .setThrowable(error.cause())
                                .log();
                    }
                })
                .match(HealthRequest.class, message -> {
                    final boolean healthy = _cluster.readView().self().status() == MemberStatus.up() && !_quarantined;
                    sender().tell(healthy, getSelf());
                })
                .build();
    }

    private void processStatusRequest() {
        // Call the bookkeeper
        final CompletableFuture<ClusterStatusCache.StatusResponse> clusterStateFuture =
                PatternsCS.ask(
                        _clusterStatusCache,
                        new ClusterStatusCache.GetRequest(),
                        Duration.ofSeconds(3))
                .thenApply(CAST_MAPPER)
                .exceptionally(new AsNullRecovery<>())
                .toCompletableFuture();

        final CompletableFuture<Map<Period, PeriodMetrics>> localMetricsFuture =
                PatternsCS.ask(
                        _localMetrics,
                        new MetricsRequest(),
                        Duration.ofSeconds(3))
                .<Map<Period, PeriodMetrics>>thenApply(new CastMapper<>())
                .exceptionally(new AsNullRecovery<>())
                .toCompletableFuture();

        PatternsCS.pipe(
                CompletableFuture.allOf(
                        clusterStateFuture.toCompletableFuture(),
                        localMetricsFuture.toCompletableFuture())
                        .thenApply(aVoid -> new StatusResponse.Builder()
                                .setClusterState(clusterStateFuture.getNow(null))
                                .setLocalMetrics(localMetricsFuture.getNow(null))
                                .setLocalAddress(_cluster.selfAddress())
                                .build()),
                context().dispatcher())
                .to(sender(), self());
    }

    private boolean _quarantined = false;

    private final Cluster _cluster;
    private final ActorRef _clusterStatusCache;
    private final ActorRef _localMetrics;

    private static final CastMapper<Object, ClusterStatusCache.StatusResponse> CAST_MAPPER = new CastMapper<>();
    private static final Logger LOGGER = LoggerFactory.getLogger(Status.class);

    private static class AsNullRecovery<T> implements Function<Throwable, T> {
        @Override
        public T apply(final Throwable failure) {
            return null;
        }
    }

    /**
     * Represents a health check request.
     */
    public static final class HealthRequest {}

    /**
     * Represents a status request.
     */
    public static final class StatusRequest {}
}
