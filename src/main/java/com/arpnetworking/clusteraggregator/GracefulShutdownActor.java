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
package com.arpnetworking.clusteraggregator;

import com.arpnetworking.clusteraggregator.client.HttpSourceActor;
import com.arpnetworking.clusteraggregator.http.Routes;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.cluster.sharding.ShardRegion;
import org.apache.pekko.pattern.Patterns;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Shuts down the Pekko cluster gracefully.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class GracefulShutdownActor extends AbstractActor {
    /**
     * Public constructor.
     *
     * @param shardRegion aggregator shard region
     * @param hostEmitter host emitter
     * @param clusterEmitter cluster emitter
     * @param routes routes
     * @param healthcheckShutdownDelay delay after shutting down healthcheck before shutting down emitters
     * @param ingestActor ingest actor
     */
    @Inject
    public GracefulShutdownActor(
            @Named("aggregator-shard-region") final ActorRef shardRegion,
            @Named("host-emitter") final ActorRef hostEmitter,
            @Named("cluster-emitter") final ActorRef clusterEmitter,
            final Routes routes,
            @Named("healthcheck-shutdown-delay") final Duration healthcheckShutdownDelay,
            @Named("http-ingest-v1") final ActorRef ingestActor) {
        _shardRegion = shardRegion;
        _hostEmitter = hostEmitter;
        _clusterEmitter = clusterEmitter;
        _routes = routes;
        _healthcheckShutdownDelay = healthcheckShutdownDelay;
        _ingestActor = ingestActor;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Shutdown.class, message -> {
                    LOGGER.info()
                            .setMessage("Initiating graceful shutdown")
                            .addData("actor", self())
                            .log();
                    self().tell(ShutdownHealthcheck.getInstance(), sender());
                })
                .match(ShutdownHealthcheck.class, message -> {
                    LOGGER.info()
                            .setMessage("Shutting down healthcheck")
                            .addData("actor", self())
                            .log();
                    _routes.shutdownHealthcheck();
                    _ingestActor.tell(HttpSourceActor.Shutdown.getInstance(), self());
                    LOGGER.info()
                            .setMessage("Waiting before proceeding with shutdown of emitters")
                            .addData("delay", _healthcheckShutdownDelay)
                            .addData("actor", self())
                            .log();
                    context().system().scheduler().scheduleOnce(
                            _healthcheckShutdownDelay,
                            self(),
                            ShutdownEmitter.getInstance(),
                            context().dispatcher(),
                            sender());
                })
                .match(ShutdownEmitter.class, message -> {
                    LOGGER.info()
                            .setMessage("Shutting down emitters")
                            .addData("actor", self())
                            .log();
                    final CompletionStage<Object> host = Patterns.ask(_hostEmitter,
                            Emitter.Shutdown.getInstance(),
                            Duration.ofSeconds(30));
                    final CompletionStage<Object> cluster = Patterns.ask(_clusterEmitter,
                            Emitter.Shutdown.getInstance(),
                            Duration.ofSeconds(30));
                    final CompletableFuture<ShutdownShardRegion> allShutdown = CompletableFuture.allOf(
                            host.toCompletableFuture(),
                            cluster.toCompletableFuture())
                            .thenApply(result -> ShutdownShardRegion.getInstance());
                    Patterns.pipe(allShutdown, context().dispatcher()).to(self(), sender());

                })
                .match(ShutdownShardRegion.class, message -> {
                    LOGGER.info()
                            .setMessage("Shutting down shard region")
                            .addData("actor", self())
                            .log();
                    context().watchWith(_shardRegion, new ShutdownShardRegionComplete(sender()));
                    _shardRegion.tell(ShardRegion.gracefulShutdownInstance(), self());
                })
                .match(ShutdownShardRegionComplete.class, terminated -> {
                    terminated._replyTo.tell("OK", self());
                    _cluster.registerOnMemberRemoved(_system::terminate);
                    _cluster.leave(_cluster.selfAddress());
                })
                .matchAny(unhandled -> {
                    LOGGER.warn()
                            .setMessage("Received unhandled message")
                            .addData("message", unhandled)
                            .addData("actor", self())
                            .log();
                })
                .build();
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        _cluster = Cluster.get(context().system());
        _system = context().system();
    }

    private final ActorRef _shardRegion;
    private final ActorRef _hostEmitter;
    private final ActorRef _clusterEmitter;
    private final Routes _routes;
    private final Duration _healthcheckShutdownDelay;
    private final ActorRef _ingestActor;
    private Cluster _cluster;
    private ActorSystem _system;
    private static final Logger LOGGER = LoggerFactory.getLogger(GracefulShutdownActor.class);
    /**
     * Message to initiate a graceful shutdown.
     */
    public static final class Shutdown {
        private Shutdown() {}
        /**
         * Gets the singleton instance of this object.
         *
         * @return a singleton instance
         */
        public static Shutdown getInstance() {
            return SHUTDOWN;
        }

        private static final Shutdown SHUTDOWN = new Shutdown();
    }
    private static final class ShutdownHealthcheck {
        private ShutdownHealthcheck() {}
        public static ShutdownHealthcheck getInstance() {
            return INSTANCE;
        }
        private static final ShutdownHealthcheck INSTANCE = new ShutdownHealthcheck();
    }
    private static final class ShutdownShardRegion {
        private ShutdownShardRegion() {}
        public static ShutdownShardRegion getInstance() {
            return INSTANCE;
        }
        private static final ShutdownShardRegion INSTANCE = new ShutdownShardRegion();
    }
    private static final class ShutdownEmitter {
        private ShutdownEmitter() {}
        public static ShutdownEmitter getInstance() {
            return INSTANCE;
        }
        private static final ShutdownEmitter INSTANCE = new ShutdownEmitter();
    }
    private static final class ShutdownShardRegionComplete {
        ShutdownShardRegionComplete(final ActorRef replyTo) {
            _replyTo = replyTo;
        }

        private final ActorRef _replyTo;
    }
}
