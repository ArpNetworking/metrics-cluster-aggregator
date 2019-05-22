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
package com.arpnetworking.clusteraggregator.http;

import akka.actor.ActorSystem;
import akka.cluster.Member;
import akka.http.javadsl.model.ContentType;
import akka.http.javadsl.model.ContentTypes;
import akka.http.javadsl.model.HttpHeader;
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.HttpRequest;
import akka.http.javadsl.model.HttpResponse;
import akka.http.javadsl.model.StatusCodes;
import akka.http.javadsl.model.headers.CacheControl;
import akka.http.javadsl.model.headers.CacheDirectives;
import akka.japi.function.Function;
import akka.pattern.PatternsCS;
import akka.util.ByteString;
import com.arpnetworking.clusteraggregator.Status;
import com.arpnetworking.clusteraggregator.models.StatusResponse;
import com.arpnetworking.clusteraggregator.models.VersionInfo;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.configuration.jackson.akka.AkkaModule;
import com.arpnetworking.metrics.Metrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Timer;
import com.arpnetworking.metrics.Units;
import com.arpnetworking.steno.LogBuilder;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.google.common.collect.ImmutableList;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

/**
 * Http server routes.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
@Singleton
public final class Routes implements Function<HttpRequest, CompletionStage<HttpResponse>> {

    /**
     * Public constructor.
     *
     * @param actorSystem Instance of <code>ActorSystem</code>.
     * @param metricsFactory Instance of <code>MetricsFactory</code>.
     * @param healthCheckPath The path for the health check.
     * @param statusPath The path for the status.
     * @param versionPath The path for the version.
     */
    @Inject
    public Routes(
            final ActorSystem actorSystem,
            final MetricsFactory metricsFactory,
            @Named("health-check-path")
            final String healthCheckPath,
            @Named("status-path")
            final String statusPath,
            @Named("version-path")
            final String versionPath) {
        _actorSystem = actorSystem;
        _metricsFactory = metricsFactory;
        _healthCheckPath = healthCheckPath;
        _statusPath = statusPath;
        _versionPath = versionPath;

        _objectMapper = ObjectMapperFactory.createInstance();
        _objectMapper.registerModule(new SimpleModule().addSerializer(Member.class, new MemberSerializer()));
        _objectMapper.registerModule(new AkkaModule(actorSystem));
    }

    @Override
    public CompletionStage<HttpResponse> apply(final HttpRequest request) {
        final Metrics metrics = _metricsFactory.create();
        final Timer timer = metrics.createTimer(createMetricName(request, REQUEST_METRIC));
        metrics.setGauge(
                createMetricName(request, BODY_SIZE_METRIC),
                request.entity().getContentLengthOption().orElse(0L),
                Units.BYTE);
        final UUID requestId = UUID.randomUUID();
        if (LOGGER.isTraceEnabled()) {
            LOGGER.trace()
                    .setEvent("http.in.start")
                    .addContext("requestId", requestId)
                    .addData("method", request.method().toString())
                    .addData("url", request.getUri().toString())
                    .addData(
                            "headers",
                            StreamSupport.stream(request.getHeaders().spliterator(), false)
                                    .map(h -> h.name() + "=" + h.value())
                                    .collect(Collectors.toList()))
                    .log();
        }
        return process(request).<HttpResponse>whenComplete(
                (response, failure) -> {
                    timer.close();

                    final int responseStatus;
                    if (response != null) {
                        responseStatus = response.status().intValue();
                    } else {
                        // TODO(ville): Figure out how to intercept post-exception mapping.
                        responseStatus = 599;
                    }
                    final int responseStatusClass = responseStatus / 100;
                    for (final int i : STATUS_CLASSES) {
                        metrics.incrementCounter(
                                createMetricName(request, String.format("%s/%dxx", STATUS_METRIC, i)),
                                responseStatusClass == i ? 1 : 0);
                    }
                    metrics.close();

                    final LogBuilder log;
                    if (failure != null || responseStatusClass == 5) {
                        log = LOGGER.info().setEvent("http.in.failure");
                        if (failure != null) {
                            log.setThrowable(failure);
                        }
                        if (!LOGGER.isTraceEnabled() && LOGGER.isInfoEnabled()) {
                            log.addData("method", request.method().toString())
                                    .addData("url", request.getUri().toString())
                                    .addData(
                                            "headers",
                                            StreamSupport.stream(request.getHeaders().spliterator(), false)
                                                    .map(h -> h.name() + "=" + h.value())
                                                    .collect(Collectors.toList()));
                        }
                    } else {
                        log = LOGGER.trace().setEvent("http.in.complete");
                    }
                    log.addContext("requestId", requestId)
                            .addData("status", responseStatus)
                            .log();
                });
    }

    private CompletionStage<HttpResponse> process(final HttpRequest request) {
        if (HttpMethods.GET.equals(request.method())) {
            if (_healthCheckPath.equals(request.getUri().path())) {
                return ask("/user/status", new Status.HealthRequest(), Boolean.FALSE)
                        .thenApply(
                                isHealthy -> HttpResponse.create()
                                        .withStatus(isHealthy ? StatusCodes.OK : StatusCodes.INTERNAL_SERVER_ERROR)
                                        .addHeader(PING_CACHE_CONTROL_HEADER)
                                        .withEntity(
                                                JSON_CONTENT_TYPE,
                                                ByteString.fromString(
                                                        "{\"status\":\""
                                                                + (isHealthy ? HEALTHY_STATE : UNHEALTHY_STATE)
                                                                + "\"}")));
            } else if (_statusPath.equals(request.getUri().path())) {
                return ask("/user/status", new Status.StatusRequest(), (StatusResponse) null)
                        .thenApply(
                                status -> {
                                    try {
                                        return HttpResponse.create()
                                                .withEntity(
                                                        JSON_CONTENT_TYPE,
                                                        ByteString.fromString(_objectMapper.writeValueAsString(status)));
                                    } catch (final IOException e) {
                                        LOGGER.error()
                                                .setMessage("Failed to serialize status")
                                                .setThrowable(e)
                                                .log();
                                        return HttpResponse.create().withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
                                    }
                                });
            } else if (_versionPath.equals(request.getUri().path())) {
                    return CompletableFuture.supplyAsync(() -> {
                            try {
                                return HttpResponse.create()
                                        .withEntity(
                                                JSON_CONTENT_TYPE,
                                                ByteString.fromString(_objectMapper.writeValueAsString(VersionInfo.getInstance())));
                            } catch (final IOException e) {
                                LOGGER.error()
                                        .setMessage("Failed to serialize version")
                                        .setThrowable(e)
                                        .log();
                                return HttpResponse.create().withStatus(StatusCodes.INTERNAL_SERVER_ERROR);
                            }
                    });
            }
        } else if (HttpMethods.POST.equals(request.method())) {
            if (INCOMING_DATA_V1_PATH.equals(request.getUri().path())
                    || INCOMING_DATA_PERSIST_V1_PATH.equals(request.getUri().path())
                    || INCOMING_DATA_REAGGREGATE_V1_PATH.equals(request.getUri().path())) {
                return ask("/user/http-ingest-v1", request, HttpResponse.create().withStatus(500));
            }
        }
        return CompletableFuture.completedFuture(HttpResponse.create().withStatus(404));
    }

    @SuppressWarnings("unchecked")
    private <T> CompletionStage<T> ask(final String actorPath, final Object request, final T defaultValue) {
        return
                PatternsCS.ask(
                        _actorSystem.actorSelection(actorPath),
                        request,
                        Duration.ofSeconds(5))
                .thenApply(o -> (T) o)
                .exceptionally(throwable -> {
                    LOGGER.error()
                            .setMessage("error when routing ask")
                            .addData("actorPath", actorPath)
                            .addData("request", request)
                            .setThrowable(throwable)
                            .log();
                    return defaultValue;
                });
    }

    private String createMetricName(final HttpRequest request, final String actionPart) {
        final StringBuilder nameBuilder = new StringBuilder()
                .append(REST_SERVICE_METRIC_ROOT)
                .append(request.method().value());
        if (!request.getUri().path().startsWith("/")) {
            nameBuilder.append("/");
        }
        nameBuilder.append(request.getUri().path());
        nameBuilder.append("/");
        nameBuilder.append(actionPart);
        return nameBuilder.toString();
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ActorSystem _actorSystem;
    @SuppressFBWarnings("SE_BAD_FIELD")
    private final MetricsFactory _metricsFactory;
    private final String _healthCheckPath;
    private final String _statusPath;
    private final String _versionPath;
    private final ObjectMapper _objectMapper;

    private static final Logger LOGGER = LoggerFactory.getLogger(Routes.class);

    private static final HttpHeader PING_CACHE_CONTROL_HEADER = CacheControl.create(
            CacheDirectives.PRIVATE(),
            CacheDirectives.NO_CACHE,
            CacheDirectives.NO_STORE,
            CacheDirectives.MUST_REVALIDATE);
    private static final String UNHEALTHY_STATE = "UNHEALTHY";
    private static final String HEALTHY_STATE = "HEALTHY";
    private static final String REST_SERVICE_METRIC_ROOT = "rest_service/";
    private static final String BODY_SIZE_METRIC = "body_size";
    private static final String REQUEST_METRIC = "request";
    private static final String STATUS_METRIC = "status";
    private static final ImmutableList<Integer> STATUS_CLASSES = ImmutableList.of(2, 3, 4, 5);

    private static final ContentType JSON_CONTENT_TYPE = ContentTypes.APPLICATION_JSON;

    private static final long serialVersionUID = -1573473630801540757L;
    
    /**
     * The legacy path for incoming data over HTTP. The reaggregation behavior
     * on this path is controlled by the the {@code calculateClusterAggregations}
     * value. If this is set to {@code False} this endpoint behaves with
     * {@link com.arpnetworking.clusteraggregator.models.AggregationMode} set to
     * {@code PERSIST}. If the configuration key is set to {@code True} this endpoint
     * behaves with {@link com.arpnetworking.clusteraggregator.models.AggregationMode}
     * set to {@code PERSIST_AND_REAGGREGATE}.
     */
    public static final String INCOMING_DATA_V1_PATH = "/metrics/v1/data";

    /**
     * This HTTP endpoint is for only persisting data directly and through the
     * host emitter.
     */
    public static final String INCOMING_DATA_PERSIST_V1_PATH = "/metrics/v1/data/persist";

    /**
     * This HTTP endpoint is for only reaggregating data and then persisting
     * through the cluster emitter.
     */
    public static final String INCOMING_DATA_REAGGREGATE_V1_PATH = "/metrics/v1/data/reaggregate";

    private static class MemberSerializer extends JsonSerializer<Member> {
        @Override
        public void serialize(
                final Member value,
                final JsonGenerator jgen,
                final SerializerProvider provider) throws IOException {
            jgen.writeStartObject();
            jgen.writeObjectField("address", value.address().toString());
            jgen.writeObjectField("roles", value.getRoles().toArray());
            jgen.writeEndObject();
        }
    }
}
