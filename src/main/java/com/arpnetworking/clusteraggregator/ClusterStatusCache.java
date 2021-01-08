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
import akka.actor.ActorSystem;
import akka.actor.Address;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.Scheduler;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ShardRegion;
import com.arpnetworking.clusteraggregator.models.ShardAllocation;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.utility.ParallelLeastShardAllocationStrategy;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.compat.java8.OptionConverters;
import scala.concurrent.duration.Duration;
import scala.concurrent.duration.FiniteDuration;

import java.io.Serializable;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import javax.annotation.Nullable;

/**
 * Caches the cluster state.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ClusterStatusCache extends AbstractActor {

    /**
     * Creates a {@link akka.actor.Props} for use in Akka.
     *
     *
     * @param system The Akka {@link ActorSystem}.
     * @param interval The {@link java.time.Duration} for polling state.
     * @param metricsFactory A {@link MetricsFactory} to use for metrics creation.
     * @return A new {@link akka.actor.Props}
     */
    public static Props props(
            final ActorSystem system,
            final java.time.Duration interval,
            final MetricsFactory metricsFactory) {
        return Props.create(ClusterStatusCache.class, system, interval, metricsFactory);
    }

    /**
     * Public constructor.
     *
     * @param system The Akka {@link ActorSystem}.
     * @param interval The {@link java.time.Duration} for polling state.
     * @param metricsFactory A {@link MetricsFactory} to use for metrics creation.
     */
    public ClusterStatusCache(
            final ActorSystem system,
            final java.time.Duration interval,
            final MetricsFactory metricsFactory) {
        _cluster = Cluster.get(system);
        _sharding = ClusterSharding.get(system);
        _interval = interval;
        _metricsFactory = metricsFactory;
    }

    @Override
    public void preStart() {
        final Scheduler scheduler = getContext()
                .system()
                .scheduler();
        _pollTimer = scheduler.schedule(
                Duration.apply(0, TimeUnit.SECONDS),
                Duration.apply(_interval.toMillis(), TimeUnit.MILLISECONDS),
                getSelf(),
                POLL,
                getContext().system().dispatcher(),
                getSelf());
    }

    @Override
    public void postStop() throws Exception {
        if (_pollTimer != null) {
            _pollTimer.cancel();
        }
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(ClusterEvent.CurrentClusterState.class, clusterState -> {
                    _clusterState = Optional.of(clusterState);
                    try (Metrics metrics = _metricsFactory.create()) {
                        metrics.setGauge("akka/members_count", clusterState.members().size());
                        if (_cluster.selfAddress().equals(clusterState.getLeader())) {
                            metrics.setGauge("akka/is_leader", 1);
                        } else {
                            metrics.setGauge("akka/is_leader", 0);
                        }
                    }
                })
                .match(ShardRegion.ClusterShardingStats.class, shardingStats -> {
                    LOGGER.debug()
                            .setMessage("Received shard statistics")
                            .addData("regionCount", shardingStats.getRegions().size())
                            .log();
                    final Map<String, Long> shardsPerAddress = Maps.newHashMap();
                    final Map<String, Long> actorsPerAddress = Maps.newHashMap();
                    for (final Map.Entry<Address, ShardRegion.ShardRegionStats> entry : shardingStats.getRegions().entrySet()) {
                        final String address = entry.getKey().hostPort();
                        for (final Map.Entry<String, Object> stats : entry.getValue().getStats().entrySet()) {
                            final long currentShardCount = shardsPerAddress.getOrDefault(address, 0L);
                            shardsPerAddress.put(address, currentShardCount + 1);
                            if (stats.getValue() instanceof Number) {
                                final long currentActorCount = actorsPerAddress.getOrDefault(address, 0L);
                                actorsPerAddress.put(
                                        address,
                                        ((Number) stats.getValue()).longValue() + currentActorCount);
                            }
                        }
                    }
                    for (final Map.Entry<String, Long> entry : shardsPerAddress.entrySet()) {
                        try (Metrics metrics = _metricsFactory.create()) {
                            final Long actorCount = actorsPerAddress.get(entry.getKey());
                            metrics.addAnnotation("address", entry.getKey());
                            metrics.setGauge("akka/shards", entry.getValue());
                            metrics.setGauge("akka/actors", actorCount);
                        }
                    }
                })
                .match(GetRequest.class, message -> sendResponse(getSender()))
                .match(ParallelLeastShardAllocationStrategy.RebalanceNotification.class, rebalanceNotification -> {
                    _rebalanceState = Optional.of(rebalanceNotification);
                })
                .matchEquals(POLL, message -> {
                    if (self().equals(sender())) {
                        _cluster.sendCurrentClusterState(getSelf());
                        for (final String shardTypeName : _sharding.getShardTypeNames()) {
                            LOGGER.debug()
                                    .setMessage("Requesting shard statistics")
                                    .addData("shardType", shardTypeName)
                                    .log();
                            _sharding.shardRegion(shardTypeName).tell(
                                    new ShardRegion.GetClusterShardingStats(FiniteDuration.fromNanos(_interval.toNanos())),
                                    self());
                        }
                    } else {
                        unhandled(message);
                    }
                })
                .build();
    }

    private void sendResponse(final ActorRef sender) {
        final StatusResponse response = new StatusResponse(
                _clusterState.orElse(_cluster.state()),
                _rebalanceState);
        sender.tell(response, self());
    }

    private static String hostFromActorRef(final ActorRef shardRegion) {
        return OptionConverters.toJava(
                shardRegion.path()
                        .address()
                        .host())
                .orElse("localhost");
    }

    private final Cluster _cluster;
    private final ClusterSharding _sharding;
    private final java.time.Duration _interval;
    private final MetricsFactory _metricsFactory;
    private Optional<ClusterEvent.CurrentClusterState> _clusterState = Optional.empty();
    @Nullable
    private Cancellable _pollTimer;
    private Optional<ParallelLeastShardAllocationStrategy.RebalanceNotification> _rebalanceState = Optional.empty();

    private static final String POLL = "poll";
    private static final Logger LOGGER = LoggerFactory.getLogger(ClusterStatusCache.class);

    /**
     * Request to get a cluster status.
     */
    public static final class GetRequest implements Serializable {
        private static final long serialVersionUID = 2804853560963013618L;
    }

    /**
     * Response to a cluster status request.
     */
    public static final class StatusResponse implements Serializable {

        /**
         * Public constructor.
         *
         * @param clusterState the cluster state
         * @param rebalanceNotification the last rebalance data
         */
        public StatusResponse(
                @Nullable final ClusterEvent.CurrentClusterState clusterState,
                final Optional<ParallelLeastShardAllocationStrategy.RebalanceNotification> rebalanceNotification) {
            _clusterState = clusterState;

            if (rebalanceNotification.isPresent()) {
                final ParallelLeastShardAllocationStrategy.RebalanceNotification notification = rebalanceNotification.get();

                // There may be a shard joining the cluster that is not in the currentAllocations list yet, but will
                // have pending rebalances to it.  Compute the set of all shard regions by unioning the current allocation list
                // with the destinations of the rebalances.
                final Set<ActorRef> allRefs = Sets.union(
                        notification.getCurrentAllocations().keySet(),
                        Sets.newHashSet(notification.getPendingRebalances().values()));

                final Map<String, ActorRef> pendingRebalances = notification.getPendingRebalances();

                final Map<ActorRef, Set<String>> currentAllocations = notification.getCurrentAllocations();

                _allocations = Optional.of(
                        allRefs.stream()
                                .map(shardRegion -> computeShardAllocation(pendingRebalances, currentAllocations, shardRegion))
                                .collect(Collectors.toList()));
            } else {
                _allocations = Optional.empty();
            }
        }

        private ShardAllocation computeShardAllocation(
                final Map<String, ActorRef> pendingRebalances,
                final Map<ActorRef, Set<String>> currentAllocations,
                final ActorRef shardRegion) {
            // Setup the map of current shard allocations
            final Set<String> currentShards = currentAllocations.getOrDefault(shardRegion, Collections.emptySet());


            // Setup the list of incoming shard allocations
            final Map<ActorRef, Collection<String>> invertPending = Multimaps
                    .invertFrom(Multimaps.forMap(pendingRebalances), ArrayListMultimap.create())
                    .asMap();
            final Set<String> incomingShards = Sets.newHashSet(invertPending.getOrDefault(shardRegion, Collections.emptyList()));

            // Setup the list of outgoing shard allocations
            final Set<String> outgoingShards = Sets.intersection(currentShards, pendingRebalances.keySet()).immutableCopy();

            // Remove the outgoing shards from the currentShards list
            currentShards.removeAll(outgoingShards);

            return new ShardAllocation.Builder()
                    .setCurrentShards(currentShards)
                    .setIncomingShards(incomingShards)
                    .setOutgoingShards(outgoingShards)
                    .setHost(hostFromActorRef(shardRegion))
                    .setShardRegion(shardRegion)
                    .build();
        }

        @Nullable
        public ClusterEvent.CurrentClusterState getClusterState() {
            return _clusterState;
        }

        public Optional<List<ShardAllocation>> getAllocations() {
            return _allocations;
        }

        @Nullable
        private final ClusterEvent.CurrentClusterState _clusterState;
        @SuppressFBWarnings("SE_BAD_FIELD")
        private final Optional<List<ShardAllocation>> _allocations;
        private static final long serialVersionUID = 603308359721162702L;
    }
}
