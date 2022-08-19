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
package com.arpnetworking.tsdcore.sinks;

import akka.actor.AbstractActor;
import akka.actor.Props;
import akka.pattern.Patterns;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.RequestEntry;
import com.google.common.base.Charsets;
import com.google.common.collect.EvictingQueue;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.asynchttpclient.AsyncCompletionHandler;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.Response;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Optional;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * Actor that sends HTTP requests via a Ning HTTP client.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HttpSinkActor extends AbstractActor {
    /**
     * Factory method to create a Props.
     *
     * @param client Http client to create requests from.
     * @param sink Sink that controls request creation and data serialization.
     * @param maximumConcurrency Maximum number of concurrent requests.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param spreadPeriod Maximum time to delay sending new aggregates to spread load.
     * @param periodicMetrics Periodic Metrics to record metrics.
     * @return A new Props
     */
    public static Props props(
            final AsyncHttpClient client,
            final HttpPostSink sink,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadPeriod,
            final PeriodicMetrics periodicMetrics) {
        return Props.create(
                HttpSinkActor.class,
                client,
                sink,
                maximumConcurrency,
                maximumQueueSize,
                spreadPeriod,
                periodicMetrics);
    }

    /**
     * Public constructor.
     *
     * @param client Http client to create requests from.
     * @param sink Sink that controls request creation and data serialization.
     * @param maximumConcurrency Maximum number of concurrent requests.
     * @param maximumQueueSize Maximum number of pending requests.
     * @param spreadPeriod Maximum time to delay sending new aggregates to spread load.
     * @param periodicMetrics Periodic Metrics to record metrics.
     */
    @SuppressFBWarnings(value = "DMI_RANDOM_USED_ONLY_ONCE", justification = "Random is used to spread load, only used once is ok")
    public HttpSinkActor(
            final AsyncHttpClient client,
            final HttpPostSink sink,
            final int maximumConcurrency,
            final int maximumQueueSize,
            final Duration spreadPeriod,
            final PeriodicMetrics periodicMetrics) {
        _client = client;
        _sink = sink;
        _acceptedStatusCodes = sink.getAcceptedStatusCodes();
        _retryableStatusCodes = sink.getRetryableStatusCodes();
        _maximumConcurrency = maximumConcurrency;
        _pendingRequests = EvictingQueue.create(maximumQueueSize);
        _periodicMetrics = periodicMetrics;
        if (Duration.ZERO.equals(spreadPeriod)) {
            _spreadingDelayMillis = 0;
        } else {
            _spreadingDelayMillis = new Random().nextInt((int) spreadPeriod.toMillis());
        }
        _evictedRequestsName = "sinks/http_post/" + _sink.getMetricSafeName() + "/evicted_requests";
        _requestLatencyName = "sinks/http_post/" + _sink.getMetricSafeName() + "/request_latency";
        _inQueueLatencyName = "sinks/http_post/" + _sink.getMetricSafeName() + "/queue_time";
        _pendingRequestsQueueSizeName = "sinks/http_post/" + _sink.getMetricSafeName() + "/queue_size";
        _inflightRequestsCountName = "sinks/http_post/" + _sink.getMetricSafeName() + "/inflight_count";
        _requestSuccessName = "sinks/http_post/" + _sink.getMetricSafeName() + "/success";
        _responseStatusName = "sinks/http_post/" + _sink.getMetricSafeName() + "/status";
        _httpSinkAttemptsName = "sinks/http_post/" + _sink.getMetricSafeName() + "/attempts";
        _samplesSentName = "sinks/http_post/" + sink.getMetricSafeName() + "/samples_sent";
        _samplesDroppedName = "sinks/http_post/" + _sink.getMetricSafeName() + "/samples_dropped";
    }

    @Override
    public void preStart() throws Exception {
        super.preStart();

        LOGGER.info()
                .setMessage("Starting http post sink actor")
                .addData("actor", this)
                .addData("actorRef", self())
                .log();

        _periodicMetrics.recordCounter("actors/http_post_sink/started", 1);
    }

    @Override
    public void postStop() throws Exception {
        super.postStop();

        LOGGER.info()
                .setMessage("Shutdown sink actor")
                .addData("actorRef", self())
                .addData("recordsWritten", _postRequests)
                .log();

        _periodicMetrics.recordCounter("actors/http_post_sink/stopped", 1);
    }

    @Override
    public void preRestart(final Throwable reason, final Optional<Object> message) throws Exception {
        super.preRestart(reason, message);

        _periodicMetrics.recordCounter("actors/http_post_sink/restarted", 1);
        LOGGER.error()
                .setMessage("Actor restarted")
                .addData("sink", _sink)
                .setThrowable(reason)
                .log();
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
                .put("acceptedStatusCodes", _acceptedStatusCodes)
                .put("retryableStatusCodes", _retryableStatusCodes)
                .put("maximumConcurrency", _maximumConcurrency)
                .put("spreadingDelayMillis", _spreadingDelayMillis)
                .put("waiting", _waiting)
                .put("inflightRequestsCount", _inflightRequestsCount)
                .put("pendingRequestsCount", _pendingRequests.size())
                .put("periodicMetrics", _periodicMetrics)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(EmitAggregation.class, this::processEmitAggregation)
                .match(PostSuccess.class, success -> {
                    processSuccessRequest(success);
                    dispatchPending();
                })
                .match(PostRejected.class, rejected -> {
                    final int attempt = rejected.getAttempt();
                    final Response response = rejected.getResponse();
                    if (_retryableStatusCodes.contains(response.getStatusCode()) && attempt < _sink.getMaximumAttempts()) {
                        POST_RETRY_LOGGER.warn()
                            .setMessage("Attempt rejected")
                            .addData("sink", _sink)
                            .addData("status", response.getStatusCode())
                            // CHECKSTYLE.OFF: IllegalInstantiation - This is ok for String from byte[]
                            .addData("request", new String(rejected.getRequestEntry().getRequest().getByteData(), Charsets.UTF_8))
                            // CHECKSTYLE.ON: IllegalInstantiation
                            .addData("response", response.getResponseBody())
                            .addContext("actor", self())
                            .log();
                        scheduleRetry(rejected.getRequestEntry(), attempt);
                    } else {
                        processRejectedRequest(rejected);
                        dispatchPending();
                    }
                })
                .match(PostFailure.class, failure -> {
                    final int attempt = failure.getAttempt();
                    if (attempt < _sink.getMaximumAttempts()) {
                        POST_RETRY_LOGGER.warn()
                                .setMessage("Attempt failed")
                                .addData("sink", _sink)
                                .addData("error", failure.getCause())
                                .addContext("actor", self())
                                .log();
                        scheduleRetry(failure.getRequestEntry(), attempt);
                    } else {
                        processFailedRequest(failure);
                        dispatchPending();
                    }
                })
                .match(Retry.class, retry -> fireRequest(retry.getRequestEntry(), retry.getAttempt()))
                .match(WaitTimeExpired.class, message -> {
                    LOGGER.debug()
                            .setMessage("Received WaitTimeExpired message")
                            .addContext("actor", self())
                            .log();
                    _waiting = false;
                    dispatchPending();
                })
                .build();
    }

    private void processFailedRequest(final PostFailure failure) {
        _inflightRequestsCount--;

        _periodicMetrics.recordCounter(_requestSuccessName, 0);
        failure.getRequestEntry().getPopulationSize().ifPresent(
                populationSize -> _periodicMetrics.recordCounter(_samplesDroppedName, populationSize));

        POST_ERROR_LOGGER.error()
                .setMessage("Post error")
                .addData("sink", _sink)
                .addContext("actor", self())
                .setThrowable(failure.getCause())
                .log();
    }

    private void processRejectedRequest(final PostRejected rejected) {
        _postRequests++;
        _inflightRequestsCount--;
        final Response response = rejected.getResponse();
        final Optional<String> responseBody = Optional.ofNullable(response.getResponseBody());
        final int responseStatusCode = response.getStatusCode();

        _periodicMetrics.recordCounter(_requestSuccessName, 0);
        rejected.getRequestEntry().getPopulationSize().ifPresent(
                populationSize -> _periodicMetrics.recordCounter(_samplesDroppedName, populationSize));
        final int responseStatusClass = responseStatusCode / 100;
        for (final int i : STATUS_CLASSES) {
            _periodicMetrics.recordCounter(
                    String.format("%s/%dxx", _responseStatusName, i),
                    responseStatusClass == i ? 1 : 0);
        }

        POST_ERROR_LOGGER.warn()
                .setMessage("Post rejected")
                .addData("sink", _sink)
                .addData("status", responseStatusCode)
                // CHECKSTYLE.OFF: IllegalInstantiation - This is ok for String from byte[]
                .addData("request", new String(rejected.getRequestEntry().getRequest().getByteData(), Charsets.UTF_8))
                // CHECKSTYLE.ON: IllegalInstantiation
                .addData("response", responseBody)
                .addContext("actor", self())
                .log();
    }

    private void processSuccessRequest(final PostSuccess success) {
        _postRequests++;
        _inflightRequestsCount--;
        final Response response = success.getResponse();
        final int responseStatusCode = response.getStatusCode();

        _periodicMetrics.recordCounter(_httpSinkAttemptsName, success.getAttempt());
        _periodicMetrics.recordCounter(_requestSuccessName, 1);
        success.getRequestEntry().getPopulationSize().ifPresent(
                populationSize -> _periodicMetrics.recordCounter(_samplesSentName, populationSize));
        final int responseStatusClass = responseStatusCode / 100;
        for (final int i : STATUS_CLASSES) {
            _periodicMetrics.recordCounter(
                    String.format("%s/%dxx", _responseStatusName, i),
                    responseStatusClass == i ? 1 : 0);
        }

        LOGGER.debug()
                .setMessage("Post accepted")
                .addData("sink", _sink)
                .addData("status", responseStatusCode)
                .addContext("actor", self())
                .log();
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
        _periodicMetrics.recordGauge(_inflightRequestsCountName, _inflightRequestsCount);

        if (!periodicData.getData().isEmpty() || !periodicData.getConditions().isEmpty()) {
            final Collection<RequestEntry.Builder> requestEntryBuilders = _sink.createRequests(_client, periodicData);
            final boolean pendingWasEmpty = _pendingRequests.isEmpty();

            final int evicted = Math.max(0, requestEntryBuilders.size() - _pendingRequests.remainingCapacity());
            for (final RequestEntry.Builder requestEntryBuilder : requestEntryBuilders) {
                // TODO(vkoskela): Add logging to client [MAI-89]
                // TODO(vkoskela): Add instrumentation to client [MAI-90]
                _pendingRequests.offer(requestEntryBuilder.setEnterTime(Instant.now()).build());
            }

            if (evicted > 0) {
                _periodicMetrics.recordCounter(_evictedRequestsName, evicted);
                EVICTED_LOGGER.warn()
                        .setMessage("Evicted data from HTTP sink queue")
                        .addData("sink", _sink)
                        .addData("count", evicted)
                        .addContext("actor", self())
                        .log();
            }

            _periodicMetrics.recordGauge(_pendingRequestsQueueSizeName, _pendingRequests.size());

            if (_spreadingDelayMillis > 0) {
                // If we don't currently have anything in-flight, we'll need to wait the spreading duration.
                if (!_waiting && pendingWasEmpty) {
                    _waiting = true;
                    LOGGER.debug()
                            .setMessage("Scheduling http requests for later transmission")
                            .addData("delayMs", _spreadingDelayMillis)
                            .addContext("actor", self())
                            .log();
                    context().system().scheduler().scheduleOnce(
                            FiniteDuration.apply(_spreadingDelayMillis, TimeUnit.MILLISECONDS),
                            self(),
                            new WaitTimeExpired(),
                            context().dispatcher(),
                            self());
                } else if (!_waiting) {
                    // If we have something in-flight continue to send without waiting
                    dispatchPending();
                }
                // Otherwise we're already waiting, these requests will be sent after the waiting is over, no need to do anything else.
            } else {
                // Spreading is disabled, just keep dispatching the work
                dispatchPending();
            }
        }
    }

    /**
     * Dispatches the number of pending requests needed to drain the pendingRequests queue or meet the maximum concurrency.
     */
    private void dispatchPending() {
        LOGGER.debug()
                .setMessage("Dispatching requests")
                .addContext("actor", self())
                .log();
        while (_inflightRequestsCount < _maximumConcurrency && !_pendingRequests.isEmpty()) {
            fireNextRequest();
        }
    }

    private void fireNextRequest() {
        final RequestEntry requestEntry = _pendingRequests.poll();
        if (requestEntry != null) {
            final long latencyInMillis = Duration.between(requestEntry.getEnterTime(), Instant.now()).toMillis();
            _periodicMetrics.recordTimer(_inQueueLatencyName, latencyInMillis, Optional.of(TimeUnit.MILLISECONDS));

            _inflightRequestsCount++;
            try {
                fireRequest(requestEntry, 1);
            // CHECKSTYLE.OFF: IllegalCatch - We need to catch everything here
            } catch (final RuntimeException e) {
            // CHECKSTYLE.ON: IllegalCatch
                _inflightRequestsCount--;
                POST_ERROR_LOGGER.error()
                        .setMessage("Error while sending request")
                        .addData("sink", _sink)
                        .addData("request", requestEntry)
                        .addContext("actor", self())
                        .setThrowable(e)
                        .log();
                throw e;
            }
        }
    }


    private void fireRequest(final RequestEntry request, final int attempt) {
        final CompletableFuture<Response> promise = new CompletableFuture<>();
        final long requestStartTime = System.currentTimeMillis();
        final CompletionStage<Object> responsePromise = promise
                .handle((result, err) -> {
                    _periodicMetrics.recordTimer(
                            _requestLatencyName,
                            System.currentTimeMillis() - requestStartTime,
                            Optional.of(TimeUnit.MILLISECONDS));
                    if (err == null) {
                        if (_acceptedStatusCodes.contains(result.getStatusCode())) {
                            return new PostSuccess(attempt, request, result);
                        } else {
                            return new PostRejected(attempt, request, result);
                        }
                    } else {
                        return new PostFailure(attempt, request, err);
                    }
                });
        Patterns.pipe(responsePromise, context().dispatcher()).to(self());
        _client.executeRequest(request.getRequest(), new ResponseAsyncCompletionHandler(promise));
    }

    private void scheduleRetry(final RequestEntry requestEntry, final int attempt) {
        final Duration delay = Duration.ofMillis(
                Math.min(
                        _sink.getRetryBaseBackoff().toMillis() * ThreadLocalRandom.current().nextInt((int) Math.pow(2, attempt - 1)),
                        _sink.getRetryMaximumDelay().toMillis()
                )

        );
        LOGGER.debug()
                .setMessage("Retry scheduled")
                .addData("request", requestEntry.getRequest())
                .addData("backoff time", delay)
                .addData("attempt", attempt)
                .addContext("actor", self())
                .log();
        getContext().system().scheduler().scheduleOnce(
                                delay,
                                self(),
                                new Retry(attempt + 1, requestEntry),
                                getContext().dispatcher(),
                                self());
    }

    private int _inflightRequestsCount = 0;
    private long _postRequests = 0;
    private boolean _waiting = false;
    private final int _maximumConcurrency;
    private final EvictingQueue<RequestEntry> _pendingRequests;
    private final AsyncHttpClient _client;
    private final HttpPostSink _sink;
    private final int _spreadingDelayMillis;
    private final PeriodicMetrics _periodicMetrics;
    private final ImmutableSet<Integer> _acceptedStatusCodes;
    private final ImmutableSet<Integer> _retryableStatusCodes;

    private final String _evictedRequestsName;
    private final String _requestLatencyName;
    private final String _inQueueLatencyName;
    private final String _pendingRequestsQueueSizeName;
    private final String _inflightRequestsCountName;
    private final String _requestSuccessName;
    private final String _responseStatusName;
    private final String _httpSinkAttemptsName;
    private final String _samplesSentName;
    private final String _samplesDroppedName;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpSinkActor.class);
    private static final Logger POST_RETRY_LOGGER = LoggerFactory.getRateLimitLogger(HttpSinkActor.class, Duration.ofSeconds(30));
    private static final Logger POST_ERROR_LOGGER = LoggerFactory.getRateLimitLogger(HttpSinkActor.class, Duration.ofSeconds(30));
    private static final Logger EVICTED_LOGGER = LoggerFactory.getRateLimitLogger(HttpSinkActor.class, Duration.ofSeconds(30));
    private static final ImmutableList<Integer> STATUS_CLASSES = com.google.common.collect.ImmutableList.of(2, 3, 4, 5);

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

    /**
     * Base class to wrap a HTTP request status.
     */
    private abstract static class PostStatus {
        private PostStatus(final int attempt) {
            _attempt = attempt;
        }

        public int getAttempt() {
            return _attempt;
        }

        private final int _attempt;
    }

    /**
     * Message class to wrap an errored HTTP request.
     */
    private static final class PostFailure extends PostStatus {
        private PostFailure(final int attempt, final RequestEntry requestEntry, final Throwable throwable) {
            super(attempt);
            _throwable = throwable;
            _requestEntry = requestEntry;
        }

        public RequestEntry getRequestEntry() {
            return _requestEntry;
        }

        public Throwable getCause() {
            return _throwable;
        }

        private final RequestEntry _requestEntry;
        private final Throwable _throwable;
    }

    /**
     * Message class to wrap a success HTTP request.
     */
    private static final class PostSuccess extends PostStatus {
        private PostSuccess(final int attempt, final RequestEntry requestEntry, final Response response) {
            super(attempt);
            _requestEntry = requestEntry;
            _response = response;
        }

        public RequestEntry getRequestEntry() {
            return _requestEntry;
        }

        public Response getResponse() {
            return _response;
        }

        private final RequestEntry _requestEntry;
        private final Response _response;
    }

    /**
     * Message class to wrap a rejected HTTP request.
     */
    private static final class PostRejected extends PostStatus {
        private PostRejected(final int attempt, final RequestEntry requestEntry, final Response response) {
            super(attempt);
            _requestEntry = requestEntry;
            _response = response;
        }

        public RequestEntry getRequestEntry() {
            return _requestEntry;
        }

        public Response getResponse() {
            return _response;
        }

        private final RequestEntry _requestEntry;
        private final Response _response;
    }

    /**
     * Message class to do retries.
     */
    private static final class Retry extends PostStatus{
        private Retry(final int attempt, final RequestEntry requestEntry) {
            super(attempt);
            _requestEntry = requestEntry;
        }

        public RequestEntry getRequestEntry() {
            return _requestEntry;
        }
        
        private final RequestEntry _requestEntry;
    }

    /**
     * Message class to indicate that we are now able to send data.
     */
    private static final class WaitTimeExpired { }

    private static final class ResponseAsyncCompletionHandler extends AsyncCompletionHandler<Response> {

        ResponseAsyncCompletionHandler(final CompletableFuture<Response> promise) {
            _promise = promise;
        }

        @Override
        public Response onCompleted(final Response response) throws Exception {
            _promise.complete(response);
            return response;
        }

        @Override
        public void onThrowable(final Throwable throwable) {
            _promise.completeExceptionally(throwable);
        }

        private final CompletableFuture<Response> _promise;
    }
}
