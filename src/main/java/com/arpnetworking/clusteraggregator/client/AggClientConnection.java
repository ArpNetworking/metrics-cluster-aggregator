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

import com.arpnetworking.clusteraggregator.models.AggregationMode;
import com.arpnetworking.clusteraggregator.models.CombinedMetricData;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.GeneratedMessageV3;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Terminated;
import org.apache.pekko.io.Tcp;
import org.apache.pekko.io.TcpMessage;
import org.apache.pekko.util.ByteString;
import scala.concurrent.duration.FiniteDuration;

import java.net.InetSocketAddress;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * An actor that handles the data sent from an agg client.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class AggClientConnection extends AbstractActor {
    /**
     * Creates a {@link Props} for use in Pekko.
     *
     * @param connection Reference to the client connection actor.
     * @param remote The address of the client socket.
     * @param maxConnectionAge The maximum duration to keep a connection open before cycling it.
     * @param calculateAggregates True to compute cluster aggregations, false to only publish host aggregations
     * @return A new {@link Props}.
     */
    public static Props props(
            final ActorRef connection,
            final InetSocketAddress remote,
            final FiniteDuration maxConnectionAge,
            final boolean calculateAggregates) {
        return Props.create(
                AggClientConnection.class,
                () -> new AggClientConnection(connection, remote, maxConnectionAge, calculateAggregates));
    }

    /**
     * Public constructor.
     *
     * @param connection Reference to the client connection actor.
     * @param remote The address of the client socket.
     * @param maxConnectionAge The maximum duration to keep a connection open before cycling it.
     * @param calculateAggregates True to compute cluster aggregations, false to only publish host aggregations
     */
    @SuppressFBWarnings(value = "MC_OVERRIDABLE_METHOD_CALL_IN_CONSTRUCTOR", justification = "context is safe to be used in constructors")
    public AggClientConnection(
            final ActorRef connection,
            final InetSocketAddress remote,
            final FiniteDuration maxConnectionAge,
            final boolean calculateAggregates) {
        _connection = connection;
        _remoteAddress = remote;
        _maxConnectionAge = maxConnectionAge;
        _calculateAggregates = calculateAggregates;

    }

    @Override
    public void preStart() throws Exception {
        super.preStart();
        context().watch(_connection);

        context().system().scheduler().scheduleOnce(
                _maxConnectionAge,
                self(),
                TcpMessage.close(),
                context().dispatcher(),
                self());
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(Tcp.Received.class, received -> {
                    final ByteString data = received.data();
                    LOGGER.trace()
                            .setMessage("Received a tcp message")
                            .addData("length", data.length())
                            .addContext("actor", self())
                            .log();
                    _buffer = _buffer.concat(data);
                    processMessages();
                })
                .match(Tcp.CloseCommand.class, message -> {
                    LOGGER.debug()
                            .setMessage("Connection timeout hit, cycling connection")
                            .addData("remote", _remoteAddress)
                            .addContext("actor", self())
                            .log();
                    if (_connection != null) {
                        _connection.tell(message, self());
                    }
                })
                .match(Tcp.ConnectionClosed.class, closed -> {
                    getContext().stop(getSelf());
                })
                .match(Terminated.class, terminated -> {
                    LOGGER.info()
                            .setMessage("Connection actor terminated")
                            .addData("terminated", terminated.actor())
                            .addContext("actor", self())
                            .log();
                    if (terminated.actor().equals(_connection)) {
                        getContext().stop(getSelf());
                    } else {
                        unhandled(terminated);
                    }
                })
                .build();
    }

    private void processMessages() {
        final AggregationMode aggregationMode =
                _calculateAggregates ? AggregationMode.PERSIST_AND_REAGGREGATE : AggregationMode.PERSIST;
        ByteString current = _buffer;
        Optional<AggregationMessage> messageOptional = AggregationMessage.deserialize(current);
        while (messageOptional.isPresent()) {
            final AggregationMessage message = messageOptional.get();
            current = current.drop(message.getLength());
            final GeneratedMessageV3 gm = message.getMessage();
            if (gm instanceof Messages.HostIdentification) {
                final Messages.HostIdentification hostIdent = (Messages.HostIdentification) gm;
                _hostName = Optional.ofNullable(hostIdent.getHostName());
                _clusterName = Optional.ofNullable(hostIdent.getClusterName());
                LOGGER.info()
                        .setMessage("Handshake received")
                        .addData("host", _hostName.orElse(""))
                        .addData("cluster", _clusterName.orElse(""))
                        .addContext("actor", self())
                        .log();
            } else if (gm instanceof Messages.StatisticSetRecord) {
                final Messages.StatisticSetRecord setRecord = (Messages.StatisticSetRecord) gm;
                LOGGER.trace()
                        .setMessage("StatisticSet record received")
                        .addData("aggregation", setRecord)
                        .addContext("actor", self())
                        .log();
                // StatisticSetRecords get forwarded to the parent, who then forwards them to the shard for cluster aggregating
                // If we aren't doing shard aggregating, don't forward it
                if (aggregationMode.shouldReaggregate()) {
                    getContext().parent().tell(setRecord, getSelf());
                }
                if (aggregationMode.shouldPersist()) {
                    if (setRecord.getStatisticsCount() > 0) {
                        final Optional<PeriodicData> periodicData = buildPeriodicData(setRecord);
                        if (periodicData.isPresent()) {
                            getContext().parent().tell(periodicData.get(), self());
                        }
                    }
                }
            } else if (gm instanceof Messages.HeartbeatRecord) {
                LOGGER.debug()
                        .setMessage("Heartbeat received")
                        .addData("host", _hostName.orElse(""))
                        .addData("cluster", _clusterName.orElse(""))
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
        final Map<String, String> dimensionsMap = setRecord.getDimensionsMap();
        final ImmutableMap.Builder<String, String> dimensionBuilder = ImmutableMap.builder();

        dimensionsMap.entrySet().stream()
                .filter(entry ->
                        !CombinedMetricData.HOST_KEY.equals(entry.getKey())
                        && !CombinedMetricData.SERVICE_KEY.equals(entry.getKey())
                        && !CombinedMetricData.CLUSTER_KEY.equals(entry.getKey()))
                .forEach(dim ->
                        dimensionBuilder.put(dim.getKey(), dim.getValue()
                ));

        Optional<String> host = Optional.ofNullable(dimensionsMap.get(CombinedMetricData.HOST_KEY));
        Optional<String> service = Optional.ofNullable(dimensionsMap.get(CombinedMetricData.SERVICE_KEY));
        Optional<String> cluster = Optional.ofNullable(dimensionsMap.get(CombinedMetricData.CLUSTER_KEY));

        if (!service.isPresent()) {
            service = Optional.ofNullable(setRecord.getService());
        }

        if (!cluster.isPresent()) {
            cluster = Optional.ofNullable(setRecord.getCluster());
            if (!cluster.isPresent()) {
                cluster = _clusterName;
            }
        }

        if (!host.isPresent()) {
            host = _hostName;
        }

        dimensionBuilder.put(CombinedMetricData.HOST_KEY, host.orElse(""));
        dimensionBuilder.put(CombinedMetricData.SERVICE_KEY, service.orElse(""));
        dimensionBuilder.put(CombinedMetricData.CLUSTER_KEY, cluster.orElse(""));

        if (!(host.isPresent() && service.isPresent() && cluster.isPresent())) {
            INCOMPLETE_RECORD_LOGGER.warn()
                    .setMessage("Cannot process StatisticSet record, missing required fields.")
                    .addData("host", host)
                    .addData("service", service)
                    .addData("cluster", cluster)
                    .log();
            return Optional.empty();
        }

        final ImmutableMap<String, String> dimensions = dimensionBuilder.build();

        final long populationSize = CombinedMetricData.computePopulationSize(
                setRecord.getMetric(),
                combinedMetricData.getCalculatedValues());

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
                    .setPopulationSize(populationSize)
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

    private Optional<String> _hostName = Optional.empty();
    private Optional<String> _clusterName = Optional.empty();
    private ByteString _buffer = ByteString.emptyByteString();
    private final ActorRef _connection;
    private final InetSocketAddress _remoteAddress;
    private final FiniteDuration _maxConnectionAge;
    private final boolean _calculateAggregates;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggClientConnection.class);
    private static final Logger INCOMPLETE_RECORD_LOGGER = LoggerFactory.getRateLimitLogger(
            AggClientConnection.class,
            Duration.ofSeconds(30));
}
