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

import com.arpnetworking.clusteraggregator.http.Routes;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Terminated;
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
     */
    @Inject
    @SuppressFBWarnings(value = "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR", justification = "Context is safe to use in constructor.")
    public GracefulShutdownActor(
            @Named("aggregator-shard-region") final ActorRef shardRegion,
            @Named("host-emitter") final ActorRef hostEmitter,
            @Named("cluster-emitter") final ActorRef clusterEmitter,
            final Routes routes,
            @Named("healthcheck-shutdown-delay") final Duration healthcheckShutdownDelay) {
        _shardRegion = shardRegion;
        _hostEmitter = hostEmitter;
        _clusterEmitter = clusterEmitter;
        _routes = routes;
        _healthcheckShutdownDelay = healthcheckShutdownDelay;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Shutdown.class, message -> {
                    LOGGER.info()
                            .setMessage("Initiating graceful shutdown")
                            .addData("actor", self())
                            .log();
                    self().tell(ShutdownHealthcheck.instance(), self());
                })
                .match(ShutdownHealthcheck.class, message -> {
                    LOGGER.info()
                            .setMessage("Shutting down healthcheck")
                            .addData("actor", self())
                            .log();
                    _routes.shutdownHealthcheck();
                    context().system().scheduler().scheduleOnce(
                            _healthcheckShutdownDelay,
                            self(),
                            ShutdownEmitter.instance(),
                            context().dispatcher(),
                            self());
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
                            .thenApply(result -> ShutdownShardRegion.instance());
                    Patterns.pipe(allShutdown, context().dispatcher()).to(self());

                })
                .match(ShutdownShardRegion.class, message -> {
                    LOGGER.info()
                            .setMessage("Shutting down shard region")
                            .addData("actor", self())
                            .log();
                    context().watch(_shardRegion);
                    _shardRegion.tell(ShardRegion.gracefulShutdownInstance(), self());
                })
                .match(Terminated.class, terminated -> {
                    _cluster.registerOnMemberRemoved(_system::terminate);
                    _cluster.leave(_cluster.selfAddress());
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
        public static Shutdown instance() {
            return SHUTDOWN;
        }

        private static final Shutdown SHUTDOWN = new Shutdown();
    }
    private static final class ShutdownHealthcheck {
        private ShutdownHealthcheck() {}
        public static ShutdownHealthcheck instance() {
            return INSTANCE;
        }
        private static final ShutdownHealthcheck INSTANCE = new ShutdownHealthcheck();
    }
    private static final class ShutdownShardRegion {
        private ShutdownShardRegion() {}
        public static ShutdownShardRegion instance() {
            return INSTANCE;
        }
        private static final ShutdownShardRegion INSTANCE = new ShutdownShardRegion();
    }
    private static final class ShutdownEmitter {
        private ShutdownEmitter() {}
        public static ShutdownEmitter instance() {
            return INSTANCE;
        }
        private static final ShutdownEmitter INSTANCE = new ShutdownEmitter();
    }
}
