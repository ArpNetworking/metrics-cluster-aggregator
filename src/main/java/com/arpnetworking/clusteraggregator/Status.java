/**
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

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.UntypedActor;
import akka.cluster.Cluster;
import akka.cluster.MemberStatus;
import akka.pattern.PatternsCS;
import akka.remote.AssociationErrorEvent;
import akka.util.Timeout;
import com.arpnetworking.clusteraggregator.models.BookkeeperData;
import com.arpnetworking.clusteraggregator.models.MetricsRequest;
import com.arpnetworking.clusteraggregator.models.PeriodMetrics;
import com.arpnetworking.clusteraggregator.models.StatusResponse;
import com.arpnetworking.utility.CastMapper;
import org.joda.time.Period;
import scala.util.Failure;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;

/**
 * Periodically polls the cluster status and caches the result.
 *
 * Accepts the following messages:
 *     STATUS: Replies with a StatusResponse message containing the service status data
 *     HEALTH: Replies with a boolean value
 *     ClusterEvent.CurrentClusterState: Updates the cached state of the cluster
 *
 * Internal-only messages:
 *     POLL: Triggers an update of the cluster data.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class Status extends UntypedActor {
    /**
     * Public constructor.
     *
     * @param metricsBookkeeper Where to get the status metrics from.
     * @param cluster The instance of the Clustering extension.
     * @param clusterStatusCache The actor holding the cached cluster status.
     * @param localMetrics The actor holding the local node metrics.
     */
    public Status(
            final ActorRef metricsBookkeeper,
            final Cluster cluster,
            final ActorRef clusterStatusCache,
            final ActorRef localMetrics) {

        _metricsBookkeeper = metricsBookkeeper;
        _cluster = cluster;
        _clusterStatusCache = clusterStatusCache;
        _localMetrics = localMetrics;
        context().system().eventStream().subscribe(self(), AssociationErrorEvent.class);
    }

    /**
     * Creates a <code>Props</code> for use in Akka.
     *
     * @param bookkeeper Where to get the status metrics from.
     * @param cluster The instance of the Clustering extension.
     * @param clusterStatusCache The actor holding the cached cluster status.
     * @param localMetrics The actor holding the local node metrics.
     * @return A new <code>Props</code>.
     */
    public static Props props(
            final ActorRef bookkeeper,
            final Cluster cluster,
            final ActorRef clusterStatusCache,
            final ActorRef localMetrics) {

        return Props.create(Status.class, bookkeeper, cluster, clusterStatusCache, localMetrics);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReceive(final Object message) throws Exception {
        final ActorRef sender = getSender();
        if (message instanceof StatusRequest) {
            processStatusRequest(sender);
        } else if (message instanceof AssociationErrorEvent) {
            final AssociationErrorEvent error = (AssociationErrorEvent) message;
            if (error.cause().getMessage().contains("quarantined this system")) {
                _quarantined = true;
            }
        } else if (message instanceof HealthRequest) {
            final CompletionStage<ClusterStatusCache.StatusResponse> stateFuture = PatternsCS
                    .ask(
                            _clusterStatusCache,
                            new ClusterStatusCache.GetRequest(),
                            Timeout.apply(3, TimeUnit.SECONDS))
                    .thenApply(CAST_MAPPER);
            stateFuture.whenComplete(
                    (statusResponse, throwable) -> {
                        final boolean healthy = MemberStatus.up().equals(_cluster.readView().self().status()) && !_quarantined;
                        sender.tell(healthy, getSelf());
                    });
        } else {
            unhandled(message);
        }
    }

    private void processStatusRequest(final ActorRef sender) {
        // Call the bookkeeper
        final CompletableFuture<BookkeeperData> bookkeeperFuture = PatternsCS.ask(
                _metricsBookkeeper,
                new MetricsRequest(),
                Timeout.apply(3, TimeUnit.SECONDS))
                .thenApply(new CastMapper<BookkeeperData>())
                .exceptionally(new AsNullRecovery<>())
                .toCompletableFuture();

        final CompletableFuture<ClusterStatusCache.StatusResponse> clusterStateFuture =
                PatternsCS.ask(
                        _clusterStatusCache,
                        new ClusterStatusCache.GetRequest(),
                        Timeout.apply(3, TimeUnit.SECONDS))
                .thenApply(CAST_MAPPER)
                .exceptionally(new AsNullRecovery<>())
                .toCompletableFuture();

        final CompletableFuture<Map<Period, PeriodMetrics>> localMetricsFuture =
                PatternsCS.ask(
                        _localMetrics,
                        new MetricsRequest(),
                        Timeout.apply(3, TimeUnit.SECONDS))
                .thenApply(new CastMapper<Map<Period, PeriodMetrics>>())
                .exceptionally(new AsNullRecovery<>())
                .toCompletableFuture();

        CompletableFuture.allOf(
                bookkeeperFuture,
                clusterStateFuture,
                localMetricsFuture)
                .thenApply(
                        (v) -> new StatusResponse.Builder()
                                .setClusterMetrics(bookkeeperFuture.getNow(null))
                                .setClusterState(clusterStateFuture.getNow(null))
                                .setLocalMetrics(localMetricsFuture.getNow(null))
                                .setLocalAddress(_cluster.selfAddress())
                                .build())
                .whenComplete(
                        (result, failure) -> {
                            if (failure != null) {
                                sender.tell(new Failure<StatusResponse>(failure), getSelf());
                            } else {
                                sender.tell(result, getSelf());
                            }
                        });
    }

    private boolean _quarantined = false;

    private final ActorRef _metricsBookkeeper;
    private final Cluster _cluster;
    private final ActorRef _clusterStatusCache;
    private final ActorRef _localMetrics;

    private static final CastMapper<ClusterStatusCache.StatusResponse> CAST_MAPPER = new CastMapper<>();

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
