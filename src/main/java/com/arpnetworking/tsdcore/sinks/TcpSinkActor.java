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

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.io.Tcp;
import akka.io.TcpExt;
import akka.io.TcpMessage;
import akka.util.ByteString;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.LinkedList;
import java.util.concurrent.TimeUnit;

/**
 * Actor that sends TCP data with Akka.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class TcpSinkActor extends AbstractActor {

    /**
     * Factory method to create a Props.
     *
     * @param sink Sink that controls request creation and data serialization.
     * @param serverAddress Server to connect to.
     * @param serverPort Port to connect to.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param exponentialBackoffBase Milliseconds as the base as the connection exponential backoff.
     * @return A new Props
     */
    public static Props props(
            final TcpSink sink,
            final String serverAddress,
            final int serverPort,
            final int maximumQueueSize,
            final Duration exponentialBackoffBase) {
        return Props.create(TcpSinkActor.class, sink, serverAddress, serverPort, maximumQueueSize, exponentialBackoffBase);
    }

    /**
     * Public constructor.
     *
     * @param sink Sink that controls request creation and data serialization.
     * @param serverAddress Server to connect to.
     * @param serverPort Port to connect to.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param exponentialBackoffBase Milliseconds as the base as the connection exponential backoff.
     */
    @SuppressFBWarnings(value = "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR", justification = "Context is safe to use in constructor.")
    public TcpSinkActor(
            final TcpSink sink,
            final String serverAddress,
            final int serverPort,
            final int maximumQueueSize,
            final Duration exponentialBackoffBase) {
        _sink = sink;
        _serverAddress = serverAddress;
        _serverPort = serverPort;
        _maximumQueueSize = maximumQueueSize;
        _exponentialBackoffBase = exponentialBackoffBase;
        _pendingRequests = new LinkedList<>();
        connect();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("sink", _sink)
                .put("serverAddress", _serverAddress)
                .put("serverPort", _serverPort)
                .put("exponentialBackoffBase", _exponentialBackoffBase)
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(EmitAggregation.class, this::processEmitAggregation)
                .match(Ack.class, ack -> {
                    _waitingForAck = false;
                    dispatchPending();
                })
                .match(Tcp.Connected.class, connected -> {
                    _client = sender();
                    _client.tell(TcpMessage.register(self(), true, true), self());
                    _connectionAttempt = 1;
                })
                .match(Tcp.CommandFailed.class, failed -> {
                    if (failed.cmd() instanceof Tcp.Connect) {
                        LOGGER.warn()
                                .setMessage("Failed to connect")
                                .addData("serverAddress", _serverAddress)
                                .addData("serverPort", _serverPort)
                                .log();
                        final long backoffMillis = (((int) (Math.random()  //randomize
                                * Math.pow(
                                EXPONENTIAL_BACKOFF_MULTIPLIER,
                                Math.min(_connectionAttempt, EXPONENTIAL_BACKOFF_MAX_EXPONENT)))) //1.3^x where x = min(attempt, 20)
                                + 1) //make sure we don't wait 0
                                * _exponentialBackoffBase.toMillis(); //the milliseconds base
                        _connectionAttempt++;
                        LOGGER.info()
                                .setMessage("Waiting to reconnect")
                                .addData("serverAddress", _serverAddress)
                                .addData("serverPort", _serverPort)
                                .addData("currentReconnectWait", backoffMillis)
                                .log();
                        context().system().scheduler().scheduleOnce(
                                FiniteDuration.apply(backoffMillis, TimeUnit.MILLISECONDS),
                                self(),
                                new Connect(),
                                context().dispatcher(),
                                self());
                    } else if (failed.cmd() instanceof Tcp.Write) {
                        final Tcp.Write write = (Tcp.Write) failed.cmd();
                        final Ack ack = (Ack) write.ack();
                        // Put the message back on the front of the queue and signal that we
                        // want to start writing again
                        _pendingRequests.offerFirst(ack._data);

                        // Potential race where a new connection could be created
                        // before the error of a previous client was surfaced, so
                        // only call resumeWriting if we haven't swapped out a new client
                        if (sender().equals(_client)) {
                            _client.tell(TcpMessage.resumeWriting(), self());
                        }
                    }
                })
                .match(Tcp.WritingResumed.class, message -> {
                    _waitingForAck = false;
                    dispatchPending();
                })
                .match(Tcp.ConnectionClosed.class, closed -> {
                    _client = null;
                    connect();
                })
                .match(Connect.class, connect -> connect())
                .build();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void postStop() throws Exception {
        super.postStop();
        LOGGER.info()
                .setMessage("Shutdown sink actor")
                .addData("sink", _sink)
                .addData("recordsWritten", _recordsWritten)
                .log();
    }

    private void connect() {
        final TcpExt tcp = Tcp.get(context().system());
        tcp.manager().tell(TcpMessage.connect(new InetSocketAddress(_serverAddress, _serverPort)), self());
    }

    private void processEmitAggregation(final EmitAggregation emitMessage) {
        final PeriodicData periodicData = emitMessage.getData();

        LOGGER.debug()
                .setMessage("Writing aggregated data")
                .addData("sink", _sink)
                .addData("dataSize", periodicData.getData().size())
                .addData("conditionsSize", periodicData.getConditions().size())
                .addContext("actor", self())
                .log();

        if (!periodicData.getData().isEmpty() || !periodicData.getConditions().isEmpty()) {
            final ByteString data = _sink.serializeData(periodicData);

            // TODO(vkoskela): Add logging to client [MAI-89]
            // TODO(vkoskela): Add instrumentation to client [MAI-90]

            if (_pendingRequests.size() >= _maximumQueueSize) {
                EVICTED_LOGGER.warn()
                        .setMessage("Evicted data from HTTP sink queue")
                        .addData("sink", _sink)
                        .addData("count", 1)
                        .addContext("actor", self())
                        .log();
                _pendingRequests.poll();
            }

            _pendingRequests.offer(data);
            dispatchPending();
        }
    }

    private void dispatchPending() {
        if (!_waitingForAck && !_pendingRequests.isEmpty() && _client != null) {
            // Push up to 10 messages
            int buffered = 0;
            ByteString data = ByteString.emptyByteString();
            while (buffered < 10 && !_pendingRequests.isEmpty()) {
                data = data.concat(_pendingRequests.poll());
                buffered++;
                _recordsWritten++;
            }
            _client.tell(TcpMessage.write(data, new Ack(data)), self());
            _waitingForAck = true;
        }
    }

    private long _recordsWritten = 0;
    private boolean _waitingForAck = false;
    private ActorRef _client = null;
    private int _connectionAttempt = 1;

    private final TcpSink _sink;
    private final String _serverAddress;
    private final int _serverPort;
    private final int _maximumQueueSize;
    private final Duration _exponentialBackoffBase;
    private final LinkedList<ByteString> _pendingRequests;

    private static final Logger LOGGER = LoggerFactory.getLogger(TcpSinkActor.class);
    private static final Logger EVICTED_LOGGER = LoggerFactory.getRateLimitLogger(TcpSinkActor.class, Duration.ofSeconds(30));

    private static final double EXPONENTIAL_BACKOFF_MULTIPLIER = 1.3;
    private static final int EXPONENTIAL_BACKOFF_MAX_EXPONENT = 20;

    private static final class Ack implements Tcp.Event {
        private Ack(final ByteString data) {
            _data = data;
        }

        private final ByteString _data;
    }

    private static final class Connect {}

    /**
     * Message class to wrap a list of {@link com.arpnetworking.tsdcore.model.AggregatedData}.
     */
    public static final class EmitAggregation {

        /**
         * Public constructor.
         *
         * @param data Periodic data to emit.
         */
        public EmitAggregation(final PeriodicData data) {
            _data = data;
        }

        public PeriodicData getData() {
            return _data;
        }

        private final PeriodicData _data;
    }
}
