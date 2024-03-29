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
     */
    @Inject
    @SuppressFBWarnings(value = "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR", justification = "Context is safe to use in constructor.")
    public GracefulShutdownActor(@Named("aggregator-shard-region") final ActorRef shardRegion) {
        _shardRegion = shardRegion;
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Shutdown.class, message -> {
                    LOGGER.info()
                            .setMessage("Initiating graceful shutdown")
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

    private ActorRef _shardRegion;
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
}
