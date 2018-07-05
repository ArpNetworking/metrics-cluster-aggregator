/*
 * Copyright 2016 Inscope Metrics, Inc
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
package com.arpnetworking.tsdcore.sinks;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.util.ByteString;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.fasterxml.jackson.annotation.JacksonInject;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;
import org.joda.time.Period;

import java.util.function.Function;

/**
 * Abstract publisher to send data to a server via Akka TCP channel.
 *
 * This class leverages a TcpSinkActor to interact with the TCP channel.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public abstract class TcpSink extends BaseSink {
    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        LOGGER.info()
                .setMessage("Closing sink")
                .addData("sink", getName())
                .log();
        _sinkActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    /**
     * {@inheritDoc}
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData data) {
        LOGGER.trace()
                .setMessage("Writing aggregated data")
                .addData("sink", getName())
                .addData("dataSize", data.getData().size())
                .addData("conditionsSize", data.getConditions().size())
                .log();

        _sinkActor.tell(new TcpSinkActor.EmitAggregation(data), ActorRef.noSender());
    }

    /**
     * Serialize a {@link PeriodicData} to binary.
     *
     * @param periodicData Data to serialize.
     * @return {@link ByteString} representing the periodicData.
     */
    protected abstract ByteString serializeData(PeriodicData periodicData);

    /**
     * Protected constructor.
     *
     * @param builder Instance of <code>Builder</code>.
     */
    protected TcpSink(final Builder<?, ?> builder) {
        super(builder);
        _sinkActor = builder._actorSystem.actorOf(
                TcpSinkActor.props(
                        this,
                        builder._serverAddress,
                        builder._serverPort,
                        builder._maxQueueSize,
                        builder._exponentialBackoffBase));
    }

    private final ActorRef _sinkActor;

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpSink.class);

    /**
     * Implementation of base builder pattern for <code>VertxSink</code>.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public abstract static class Builder<B extends BaseSink.Builder<B, S>, S extends Sink> extends BaseSink.Builder<B, S> {

        /**
         * The server host name. Cannot be null or empty.
         *
         * @param value The aggregation server host name.
         * @return This instance of <code>Builder</code>.
         */
        public B setServerAddress(final String value) {
            _serverAddress = value;
            return self();
        }

        /**
         * The server port. Cannot be null; must be between 1 and 65535.
         *
         * @param value The server port.
         * @return This instance of <code>Builder</code>.
         */
        public B setServerPort(final Integer value) {
            _serverPort = value;
            return self();
        }

        /**
         * The maximum queue size. Cannot be null. Default is 10000.
         *
         * @param value The maximum queue size.
         * @return This instance of <code>Builder</code>.
         */
        public B setMaxQueueSize(final Integer value) {
            _maxQueueSize = value;
            return self();
        }

        /**
         * The actor system to run the actors in. Required. Cannot be null.
         *
         * @param value An actor system
         * @return This instance of <code>Builder</code>.
         */
        public B setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return self();
        }

        /**
         * Protected constructor for subclasses.
         *
         * @param targetConstructor The constructor for the concrete type to be created by this builder.
         */
        protected Builder(final Function<B, S> targetConstructor) {
            super(targetConstructor);
        }

        @NotNull
        @NotEmpty
        private String _serverAddress;
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _serverPort;
        @JacksonInject
        @NotNull
        private ActorSystem _actorSystem;
        @NotNull
        @Min(value = 0)
        private Integer _maxQueueSize = 10000;
        @NotNull
        private Period _exponentialBackoffBase = Period.millis(500);
    }
}
