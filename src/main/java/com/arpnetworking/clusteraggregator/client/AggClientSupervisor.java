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

package com.arpnetworking.clusteraggregator.client;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.AllForOneStrategy;
import akka.actor.SupervisorStrategy;
import akka.actor.Terminated;
import akka.io.Tcp;
import akka.io.TcpMessage;
import com.arpnetworking.clusteraggregator.configuration.ClusterAggregatorConfiguration;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Random;
import java.util.concurrent.TimeUnit;

/**
 * Supervises the connection's actors.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class AggClientSupervisor extends AbstractActor {
    /**
     * Public constructor.
     *
     * @param shardRegion The aggregator shard region actor.
     * @param emitter The emitter actor.
     * @param configuration The cluster aggregator configuration.
     */
    @Inject
    public AggClientSupervisor(
            @Named("aggregator-shard-region") final ActorRef shardRegion,
            @Named("host-emitter") final ActorRef emitter,
            final ClusterAggregatorConfiguration configuration) {
        _shardRegion = shardRegion;
        _emitter = emitter;
        _minConnectionTimeout = configuration.getMinConnectionTimeout();
        _maxConnectionTimeout = configuration.getMaxConnectionTimeout();
        _calculateClusterAggregates = configuration.getCalculateClusterAggregations();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                // Route the host data to the emitter
                .match(AggregatedData.class, message -> _emitter.forward(message, context()))
                .match(PeriodicData.class, message -> _emitter.forward(message, context()))
                // Route the message to the sharding region
                .match(Messages.StatisticSetRecord.class, message -> _shardRegion.forward(message, context()))
                .match(Tcp.Connected.class, connected -> {
                    final ActorRef connection = getSender();

                    final ActorRef handler = getContext().actorOf(
                            AggClientConnection.props(
                                    connection,
                                    connected.remoteAddress(),
                                    getRandomConnectionTime(),
                                    _calculateClusterAggregates),
                            "dataHandler");
                    connection.tell(TcpMessage.register(handler, true, true), getSelf());
                    getContext().watch(handler);
                })
                .match(Terminated.class, message -> {
                    LOGGER.debug()
                            .setMessage("Handler shutdown., shutting down supervisor")
                            .addContext("actor", self())
                            .log();
                    getContext().stop(getSelf());
                })
                .build();
    }

    private FiniteDuration getRandomConnectionTime() {
        final long minMillis = _minConnectionTimeout.toMillis();
        final long maxMillis = _maxConnectionTimeout.toMillis();
        final long randomMillis = (long) (_random.nextDouble() * (maxMillis - minMillis) + minMillis);
        return FiniteDuration.apply(randomMillis, TimeUnit.MILLISECONDS);
    }

    @Override
    public SupervisorStrategy supervisorStrategy() {
        // The important part of AllForOneStrategy is the decider lambda.  The number of retries and the timeframe
        // will only be used if the lambda returns null.  In our case, the lambda will stop the actor on any exception
        // so the retries count and timeframe are irrelevant.
        return new AllForOneStrategy(
                1, // Number of retries
                Duration.of(5, ChronoUnit.MINUTES), // Within 5 minutes
                throwable -> {
                    LOGGER.warn()
                            .setMessage("Supervisor caught exception")
                            .setThrowable(throwable)
                            .addContext("actor", self())
                            .log();
                     //if any of the children throw an exception, stop this actor to clean up all the resources
                     // the client will need to reconnect
                     return SupervisorStrategy.stop();
                });
    }

    private ActorRef _handler;
    private InetSocketAddress _remote;

    private final ActorRef _shardRegion;
    private final ActorRef _emitter;
    private final Duration _minConnectionTimeout;
    private final Duration _maxConnectionTimeout;
    private final Boolean _calculateClusterAggregates;
    private final Random _random = new Random();
    private static final Logger LOGGER = LoggerFactory.getLogger(AggClientSupervisor.class);
}
