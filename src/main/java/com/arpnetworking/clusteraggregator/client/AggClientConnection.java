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

package com.arpnetworking.clusteraggregator.client;

import akka.actor.ActorRef;
import akka.actor.Props;
import akka.actor.Terminated;
import akka.actor.UntypedActor;
import akka.io.Tcp;
import akka.io.TcpMessage;
import akka.util.ByteString;
import com.arpnetworking.clusteraggregator.models.CombinedMetricData;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessage;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * An actor that handles the data sent from an agg client.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class AggClientConnection extends UntypedActor {
    /**
     * Creates a <code>Props</code> for use in Akka.
     *
     * @param connection Reference to the client connection actor.
     * @param remote The address of the client socket.
     * @param maxConnectionAge The maximum duration to keep a connection open before cycling it.
     * @return A new <code>Props</code>.
     */
    public static Props props(final ActorRef connection, final InetSocketAddress remote, final FiniteDuration maxConnectionAge) {
        return Props.create(AggClientConnection.class, connection, remote, maxConnectionAge);
    }

    /**
     * Public constructor.
     *
     * @param connection Reference to the client connection actor.
     * @param remote The address of the client socket.
     * @param maxConnectionAge The maximum duration to keep a connection open before cycling it.
     */
    public AggClientConnection(
            final ActorRef connection,
            final InetSocketAddress remote,
            final FiniteDuration maxConnectionAge) {
        _connection = connection;
        _remoteAddress = remote;

        getContext().watch(connection);

        context().system().scheduler().scheduleOnce(
                maxConnectionAge,
                self(),
                TcpMessage.close(),
                context().dispatcher(),
                self());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onReceive(final Object message) throws Exception {
        if (message instanceof Tcp.Received) {
            final Tcp.Received received = (Tcp.Received) message;
            final ByteString data = received.data();
            LOGGER.trace()
                    .setMessage("Received a tcp message")
                    .addData("length", data.length())
                    .addContext("actor", self())
                    .log();
            _buffer = _buffer.concat(data);
            processMessages();
        } else if (message instanceof Tcp.CloseCommand) {
            LOGGER.debug()
                    .setMessage("Connection timeout hit, cycling connection")
                    .addData("remote", _remoteAddress)
                    .addContext("actor", self())
                    .log();
            if (_connection != null) {
                _connection.tell(message, self());
            }
        } else if (message instanceof Tcp.ConnectionClosed) {
            getContext().stop(getSelf());
        } else if (message instanceof Terminated) {
            final Terminated terminated = (Terminated) message;
            LOGGER.info()
                    .setMessage("Connection actor terminated")
                    .addData("terminated", terminated.actor())
                    .addContext("actor", self())
                    .log();
            if (terminated.actor().equals(_connection)) {
                getContext().stop(getSelf());
            } else {
                unhandled(message);
            }
        } else {
            unhandled(message);
        }
    }

    private void processMessages() {
        ByteString current = _buffer;
        Optional<AggregationMessage> messageOptional = AggregationMessage.deserialize(current);
        while (messageOptional.isPresent()) {
            final AggregationMessage message = messageOptional.get();
            current = current.drop(message.getLength());
            final GeneratedMessage gm = message.getMessage();
            if (gm instanceof Messages.HostIdentification) {
                final Messages.HostIdentification hostIdent = (Messages.HostIdentification) gm;
                if (hostIdent.hasHostName()) {
                    _hostName = Optional.fromNullable(hostIdent.getHostName());
                }
                if (hostIdent.hasClusterName()) {
                    _clusterName = Optional.fromNullable(hostIdent.getClusterName());
                }
                LOGGER.info()
                        .setMessage("Handshake received")
                        .addData("host", _hostName.or(""))
                        .addData("cluster", _clusterName.or(""))
                        .addContext("actor", self())
                        .log();
            } else if (gm instanceof Messages.StatisticSetRecord) {
                final Messages.StatisticSetRecord setRecord = (Messages.StatisticSetRecord) gm;
                LOGGER.trace()
                        .setMessage("StatisticSet record received")
                        .addData("aggregation", setRecord)
                        .addContext("actor", self())
                        .log();
                getContext().parent().tell(setRecord, getSelf());
                if (setRecord.getStatisticsCount() > 0) {
                    final Optional<PeriodicData> periodicData = buildPeriodicData(setRecord);
                    if (periodicData.isPresent()) {
                        getContext().parent().tell(periodicData.get(), self());
                    }
                }
            } else if (gm instanceof Messages.HeartbeatRecord) {
                LOGGER.debug()
                        .setMessage("Heartbeat received")
                        .addData("host", _hostName.or(""))
                        .addData("cluster", _clusterName.or(""))
                        .addContext("actor", self())
                        .log();
            } else {
                LOGGER.warn()
                        .setMessage("Unknown message type")
                        .addData("type", gm.getClass())
                        .addContext("actor", self())
                        .log();
            }
            messageOptional = AggregationMessage.deserialize(current);
            if (!messageOptional.isPresent() && current.length() > 4) {
                LOGGER.debug()
                        .setMessage("buffer did not deserialize completely")
                        .addData("remainingBytes", current.length())
                        .addContext("actor", self())
                        .log();
            }
        }
        //TODO(barp): Investigate using a ring buffer [MAI-196]
        _buffer = current;
    }

    private Optional<PeriodicData> buildPeriodicData(final Messages.StatisticSetRecord setRecord) {
        final CombinedMetricData combinedMetricData = CombinedMetricData.Builder.fromStatisticSetRecord(setRecord).build();
        final ImmutableList.Builder<AggregatedData> builder = ImmutableList.builder();
        final ImmutableMap.Builder<String, String> dimensionBuilder = ImmutableMap.builder();

        Optional<String> host = Optional.absent();
        Optional<String> service = Optional.absent();
        Optional<String> cluster = Optional.absent();
        for (final Messages.DimensionEntry dimensionEntry : setRecord.getDimensionsList()) {
            if (HOST_KEY.equals(dimensionEntry.getKey())) {
                host = Optional.fromNullable(dimensionEntry.getValue());
            } else if (SERVICE_KEY.equals(dimensionEntry.getKey())) {
                service = Optional.fromNullable(dimensionEntry.getValue());
            } else if (CLUSTER_KEY.equals(dimensionEntry.getKey())) {
                cluster = Optional.fromNullable(dimensionEntry.getValue());
            } else {
                dimensionBuilder.put(dimensionEntry.getKey(), dimensionEntry.getValue());
            }
        }

        if (!service.isPresent()) {
            service = Optional.fromNullable(setRecord.getService());
        }

        if (!cluster.isPresent()) {
            cluster = Optional.fromNullable(setRecord.getCluster());
            if (!cluster.isPresent()) {
                cluster = _clusterName;
            }
        }

        if (!host.isPresent()) {
            host = _hostName;
        }

        dimensionBuilder.put(HOST_KEY, host.or(""));
        dimensionBuilder.put(SERVICE_KEY, service.or(""));
        dimensionBuilder.put(CLUSTER_KEY, cluster.or(""));

        if (!(host.isPresent() && service.isPresent() && cluster.isPresent())) {
            INCOMPLETE_RECORD_LOGGER.warn()
                    .setMessage("Cannot process StatisticSet record, missing required fields.")
                    .addData("host", host)
                    .addData("service", service)
                    .addData("cluster", cluster)
                    .log();
            return Optional.absent();
        }

        final ImmutableMap<String, String> dimensions = dimensionBuilder.build();

        for (final Map.Entry<Statistic, CombinedMetricData.StatisticValue> record
                : combinedMetricData.getCalculatedValues().entrySet()) {
            final AggregatedData aggregatedData = new AggregatedData.Builder()
                    .setFQDSN(new FQDSN.Builder()
                            .setCluster(setRecord.getCluster())
                            .setMetric(setRecord.getMetric())
                            .setService(setRecord.getService())
                            .setStatistic(record.getKey())
                            .build())
                    .setHost(host.get())
                    .setIsSpecified(record.getValue().getUserSpecified())
                    .setPeriod(combinedMetricData.getPeriod())
                    .setPopulationSize(1L)
                    .setSamples(Collections.emptyList())
                    .setStart(combinedMetricData.getPeriodStart())
                    .setSupportingData(record.getValue().getValue().getData())
                    .setValue(record.getValue().getValue().getValue())
                    .build();
            builder.add(aggregatedData);
        }
        return Optional.of(new PeriodicData.Builder()
                .setData(builder.build())
                .setConditions(ImmutableList.of())
                .setDimensions(dimensions)
                .setPeriod(combinedMetricData.getPeriod())
                .setStart(combinedMetricData.getPeriodStart())
                .build());
    }

    private Optional<String> _hostName = Optional.absent();
    private Optional<String> _clusterName = Optional.absent();
    private ByteString _buffer = ByteString.empty();
    private final ActorRef _connection;
    private final InetSocketAddress _remoteAddress;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggClientConnection.class);
    private static final Logger INCOMPLETE_RECORD_LOGGER = LoggerFactory.getRateLimitLogger(
            AggClientConnection.class,
            Duration.ofSeconds(30));
    private static final boolean IS_ENABLED;
    /**
     * The key used in dimensions to specify the host-dimension.
     */
    public static final String HOST_KEY = "host";
    /**
     * The key used in dimensions to specify the service-dimension.
     */
    public static final String SERVICE_KEY = "service";
    /**
     * The key used in dimensions to specify the cluster-dimension.
     */
    public static final String CLUSTER_KEY = "cluster";


    static {
        // Determine the local host name
        String localHost = "UNKNOWN";
        try {
            localHost = InetAddress.getLocalHost().getCanonicalHostName();
            LOGGER.info(String.format("Determined local host name as: %s", localHost));
        } catch (final UnknownHostException e) {
            LOGGER.warn("Unable to determine local host name", e);
        }

        // Determine if the host name is enabled
        IS_ENABLED = Pattern.matches(".*\\.lup1$", localHost) || Pattern.matches(".*\\.snc1$", localHost);
        LOGGER.info(String.format("Cluster aggregator will be %s", IS_ENABLED ? "ENABLED" : "DISABLED"));
    }
}
