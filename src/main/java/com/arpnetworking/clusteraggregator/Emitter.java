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

import com.arpnetworking.clusteraggregator.configuration.EmitterConfiguration;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.sinks.MultiSink;
import com.arpnetworking.tsdcore.sinks.Sink;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;
import org.apache.pekko.pattern.Patterns;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.concurrent.CompletionStage;

/**
 * Holds the sinks and emits to them.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class Emitter extends AbstractActor {
    /**
     * Creates a {@link Props} for construction in Pekko.
     *
     * @param config Config describing the sinks to write to
     * @return A new {@link Props}.
     */
    public static Props props(final EmitterConfiguration config) {
        return Props.create(Emitter.class, () -> new Emitter(config));
    }

    /**
     * Public constructor.
     *
     * @param config Config describing the sinks to write to
     */
    public Emitter(final EmitterConfiguration config) {
        _sink = new MultiSink.Builder()
                .setName("EmitterMultiSink")
                .setSinks(config.getSinks())
                .build();
        LOGGER.info()
                .setMessage("Emitter starting up")
                .addData("sink", _sink)
                .log();
    }

    @Override
    public void preStart() throws Exception, Exception {
        super.preStart();
    }

    @SuppressWarnings("deprecation")
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AggregatedData.class, datum -> {
                    final String host = datum.getHost();
                    final Duration period = datum.getPeriod();
                    final ZonedDateTime start = datum.getStart();
                    final PeriodicData periodicData = new PeriodicData.Builder()
                            .setData(ImmutableList.of(datum))
                            .setConditions(ImmutableList.of())
                            .setDimensions(ImmutableMap.of("host", host))
                            .setPeriod(period)
                            .setStart(start)
                            .build();
                    LOGGER.trace()
                            .setMessage("Emitting data to sink")
                            .addData("data", datum)
                            .log();
                    _sink.recordAggregateData(periodicData);
                })
                .match(PeriodicData.class, periodicData -> {
                    LOGGER.trace()
                            .setMessage("Emitting data to sink")
                            .addData("data", periodicData)
                            .log();
                    _sink.recordAggregateData(periodicData);
                })
                .match(Shutdown.class, ignored -> {
                    LOGGER.info()
                            .setMessage("Shutting down emitter")
                            .log();

                    final CompletionStage<Object> shutdownFuture = _sink.shutdownGracefully()
                            .thenApply(ignore -> ShutdownComplete.getInstance());
                    Patterns.pipe(shutdownFuture, context().dispatcher()).to(self(), sender());
                })
                .match(ShutdownComplete.class, ignored -> {
                    LOGGER.info()
                            .setMessage("Emitter shutdown complete")
                            .log();
                    sender().tell("OK", self());
                    context().stop(self());
                })
                .build();
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();
        _sink.close();
    }

    private final Sink _sink;
    private static final Logger LOGGER = LoggerFactory.getLogger(Emitter.class);
    /**
     * Message to initiate a graceful shutdown.
     */
    public static final class Shutdown {
        private Shutdown() {}

        /**
         * Get the singleton instance.
         *
         * @return the singleton instance
         */
        public static Shutdown getInstance() {
            return INSTANCE;
        }
        private static final Shutdown INSTANCE = new Shutdown();
    }
    private static final class ShutdownComplete {
        private ShutdownComplete() {}

        /**
         * Get the singleton instance.
         *
         * @return the singleton instance
         */
        public static ShutdownComplete getInstance() {
            return INSTANCE;
        }
        private static final ShutdownComplete INSTANCE = new ShutdownComplete();
    }
}
