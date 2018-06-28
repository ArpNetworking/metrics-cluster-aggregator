/**
 * Copyright 2018 Inscope Metrics, Inc
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

import akka.Done;
import akka.NotUsed;
import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.Props;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.RequestEntity;
import akka.japi.Pair;
import akka.stream.ActorMaterializer;
import akka.stream.ActorMaterializerSettings;
import akka.stream.FanInShape2;
import akka.stream.FlowShape;
import akka.stream.Graph;
import akka.stream.Materializer;
import akka.stream.Supervision;
import akka.stream.UniformFanOutShape;
import akka.stream.javadsl.Broadcast;
import akka.stream.javadsl.Flow;
import akka.stream.javadsl.GraphDSL;
import akka.stream.javadsl.Keep;
import akka.stream.javadsl.Sink;
import akka.stream.javadsl.Source;
import akka.stream.javadsl.Zip;
import akka.util.ByteString;
import com.arpnetworking.clusteraggregator.configuration.ClusterAggregatorConfiguration;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.google.protobuf.GeneratedMessageV3;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * Source that uses HTTP POSTs as input.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HttpSourceActor extends AbstractActor {
    /**
     * Creates a {@link Props} for this actor.
     *
     * @param shardRegion The aggregator shard region actor.
     * @param emitter actor that emits the host data
     * @param configuration The cluster aggregator configuration.
     * @return A new {@link Props}
     */
    /* package private */ static Props props(
            final ActorRef shardRegion,
            final ActorRef emitter,
            final ClusterAggregatorConfiguration configuration) {
        return Props.create(() -> new HttpSourceActor(shardRegion, emitter, configuration));
    }

    /**
     * Public constructor.
     * @param shardRegion The aggregator shard region actor.
     * @param emitter actor that emits the host data
     * @param configuration The cluster aggregator configuration.
     */
    @Inject
    public HttpSourceActor(
            @Named("aggregator-shard-region") final ActorRef shardRegion,
            @Named("host-emitter") final ActorRef emitter,
            final ClusterAggregatorConfiguration configuration) {

        final ActorRef self = self();
        _sink = Sink.foreach(msg -> {
            final GeneratedMessageV3 generatedMessageV3 = msg.getMessage();
            if (generatedMessageV3 instanceof Messages.StatisticSetRecord) {
                if (configuration.getCalculateClusterAggregations()) {
                    shardRegion.tell(msg, self);
                }
                emitter.tell(msg, self);
            }
        });

        _materializer = ActorMaterializer.create(
                ActorMaterializerSettings.create(context().system())
                        .withSupervisionStrategy(Supervision.stoppingDecider()),
                context());

        _processGraph = GraphDSL.create(builder -> {

            // Flows
            final Flow<HttpRequest, ByteString, NotUsed> getBodyFlow = Flow.<HttpRequest>create()
                    .map(HttpRequest::entity)
                    .flatMapConcat(RequestEntity::getDataBytes)
                    .reduce(ByteString::concat)
                    .named("getBody");

            final Flow<HttpRequest, ImmutableMultimap<String, String>, NotUsed> getHeadersFlow = Flow.<HttpRequest>create()
                    .map(HttpRequest::getHeaders)
                    .map(HttpSourceActor::createHeaderMultimap) // Transform to array form
                    .named("getHeaders");

            final Flow<Pair<ByteString, ImmutableMultimap<String, String>>, AggregationMessage, NotUsed> createAndParseFlow =
                    Flow.<Pair<ByteString, ImmutableMultimap<String, String>>>create()
                            .map(HttpSourceActor::mapModel)
                            .mapConcat(this::parseRecords) // Parse the json string into a record builder
                            // NOTE: this should be _parser::parse, but aspectj NPEs with that currently
                            .named("createAndParseRequest");

            // Shapes
            final UniformFanOutShape<HttpRequest, HttpRequest> split = builder.add(Broadcast.create(2));

            final FlowShape<HttpRequest, ByteString> getBody = builder.add(getBodyFlow);
            final FlowShape<HttpRequest, ImmutableMultimap<String, String>> getHeaders = builder.add(getHeadersFlow);
            final FanInShape2<
                    ByteString,
                    ImmutableMultimap<String, String>,
                    Pair<ByteString, ImmutableMultimap<String, String>>> join = builder.add(Zip.create());
            final FlowShape<Pair<ByteString, ImmutableMultimap<String, String>>, AggregationMessage> createRequest =
                    builder.add(createAndParseFlow);

            // Wire the shapes
            builder.from(split.out(0)).via(getBody).toInlet(join.in0()); // Split to get the body bytes
            builder.from(split.out(1)).via(getHeaders).toInlet(join.in1()); // Split to get the headers
            builder.from(join.out()).toInlet(createRequest.in()); // Join to create the Request and parse it

            return FlowShape.of(split.in(), createRequest.out());
        });
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(HttpRequest.class, request -> {
                    final ActorRef sender = sender();
                    Source.single(request)
                            .via(_processGraph)
                            .toMat(_sink, Keep.right())
                            .run(_materializer)
                            .whenComplete((done, err) -> {
                                if (err == null) {
                                    sender.tell(HttpResponse.create().withStatus(200), self());
                                } else {
                                    if (err instanceof NoRecordsException) {
                                        sender.tell(HttpResponse.create().withStatus(400), self());
                                    } else {
                                        BAD_REQUEST_LOGGER.warn()
                                                .setMessage("Error handling http post")
                                                .setThrowable(err)
                                                .log();
                                        sender.tell(HttpResponse.create().withStatus(500), self());
                                    }
                                }
                            });
                })
                .build();
    }

    private static ImmutableMultimap<String, String> createHeaderMultimap(final Iterable<HttpHeader> headers) {
        final ImmutableMultimap.Builder<String, String> headersBuilder = ImmutableMultimap.builder();

        for (final HttpHeader httpHeader : headers) {
            headersBuilder.put(httpHeader.lowercaseName(), httpHeader.value());
        }

        return headersBuilder.build();
    }

    private static com.arpnetworking.clusteraggregator.models.HttpRequest mapModel(
            final Pair<ByteString, ImmutableMultimap<String, String>> pair) {
        return new com.arpnetworking.clusteraggregator.models.HttpRequest(pair.second(), pair.first());
    }

    private List<AggregationMessage> parseRecords(final com.arpnetworking.clusteraggregator.models.HttpRequest request) {
        final ArrayList<AggregationMessage> records = Lists.newArrayList();
        ByteString current = request.getBody();
        Optional<AggregationMessage> messageOptional = AggregationMessage.deserialize(current);
        while (messageOptional.isPresent()) {
            final AggregationMessage message = messageOptional.get();
            records.add(message);
            current = current.drop(message.getLength());
            messageOptional = AggregationMessage.deserialize(current);
            if (!messageOptional.isPresent() && current.lengthCompare(4) < 0) {
                LOGGER.debug()
                        .setMessage("buffer did not deserialize completely")
                        .addData("remainingBytes", current.size())
                        .addContext("actor", self())
                        .log();
            }
        }
        if (records.size() == 0) {
            throw new NoRecordsException();
        }
        return records;
    }

    private final Materializer _materializer;
    private final Sink<AggregationMessage, CompletionStage<Done>> _sink;
    private final Graph<FlowShape<HttpRequest, AggregationMessage>, NotUsed> _processGraph;

    private static final Logger BAD_REQUEST_LOGGER =
                LoggerFactory.getRateLimitLogger(HttpSourceActor.class, Duration.ofSeconds(30));
    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSourceActor.class);


    private static class NoRecordsException extends RuntimeException {
        NoRecordsException() {
        }

        NoRecordsException(final String message) {
            super(message);
        }

        NoRecordsException(final String message, final Throwable cause) {
            super(message, cause);
        }

        NoRecordsException(final Throwable cause) {
            super(cause);
        }

        NoRecordsException(final String message, final Throwable cause, final boolean enableSuppression,
                final boolean writableStackTrace) {
            super(message, cause, enableSuppression, writableStackTrace);
        }

        private static final long serialVersionUID = 1L;
    }
}

