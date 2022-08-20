/*
 * Copyright 2014 Brandon Arp
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
import akka.http.javadsl.model.HttpMethods;
import akka.http.javadsl.model.MediaTypes;
import akka.http.javadsl.model.StatusCodes;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.RequestEntry;
import com.fasterxml.jackson.annotation.JacksonInject;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import net.sf.oval.Validator;
import net.sf.oval.constraint.CheckWith;
import net.sf.oval.constraint.CheckWithCheck;
import net.sf.oval.constraint.Min;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.context.OValContext;
import org.asynchttpclient.AsyncHttpClient;
import org.asynchttpclient.AsyncHttpClientConfig;
import org.asynchttpclient.DefaultAsyncHttpClient;
import org.asynchttpclient.DefaultAsyncHttpClientConfig;
import org.asynchttpclient.Request;
import org.asynchttpclient.RequestBuilder;
import org.asynchttpclient.uri.Uri;

import java.io.Serial;
import java.net.URI;
import java.time.Duration;
import java.util.Collection;
import java.util.Optional;
import java.util.function.Function;

/**
 * Publishes to an HTTP endpoint. This class is thread safe.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public abstract class HttpPostSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData data) {
        _sinkActor.tell(new HttpSinkActor.EmitAggregation(data), ActorRef.noSender());
    }

    @Override
    public void close() {
        LOGGER.info()
                .setMessage("Closing sink")
                .addData("sink", getName())
                .addData("uri", _uri)
                .log();
        _sinkActor.tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("actor", _sinkActor)
                .put("uri", _uri)
                .build();
    }

    /**
     * Creates an HTTP request from a serialized data entry. Default is an {@code POST} containing
     * serializedData as the body with content type of application/json
     *
     * @param client The http client to build the request with.
     * @param serializedData The serialized data.
     * @return {@link Request} to execute
     */
    protected Request createRequest(final AsyncHttpClient client, final byte[] serializedData) {
        return new RequestBuilder()
                .setUri(_aysncHttpClientUri)
                .setHeader("Content-Type", MediaTypes.APPLICATION_JSON.toString())
                .setBody(serializedData)
                .setMethod(HttpMethods.POST.value())
                .build();
    }

    /**
     * Create HTTP requests for each serialized data entry. The list is
     * guaranteed to be non-empty.
     *
     * @param client The http client to build the request with.
     * @param periodicData The {@link PeriodicData} to be serialized.
     * @return The {@link Request} instance to execute.
     */
    protected Collection<RequestEntry.Builder> createRequests(
            final AsyncHttpClient client,
            final PeriodicData periodicData) {
        final Collection<SerializedDatum> serializedData = serialize(periodicData);
        final Collection<RequestEntry.Builder> requestEntryBuilders = Lists.newArrayListWithExpectedSize(serializedData.size());
        for (final SerializedDatum serializedDatum : serializedData) {
            // TODO(ville): Convert RequestEntry.Builder would be a ThreadLocalBuilder
            // Unfortunately, the split builder logic across HttpPostSink and
            // HttpSinkActor does not permit this as-is. The logic would need
            // to be refactored to permit the use of a TLB.
            requestEntryBuilders.add(new RequestEntry.Builder()
                    .setRequest(createRequest(client, serializedDatum.getDatum()))
                    .setPopulationSize(serializedDatum.getPopulationSize()));
        }
        return requestEntryBuilders;
    }

    /**
     * Accessor for the {@link URI}.
     *
     * @return The {@link URI}.
     */
    protected URI getUri() {
        return _uri;
    }
    
    /**
     * Accessor for the AysncHttpClient {@link Uri}.
     *
     * @return The AysncHttpClient {@link Uri}.
     */
    protected Uri getAysncHttpClientUri() {
        return _aysncHttpClientUri;
    }
    
    /**
     * Accessor for the MaximumAttempts.
     *
     * @return The MaximumAttempts.
     */
    protected int getMaximumAttempts() {
        return _maximumAttempts;
    }

    /**
     * Accessor for the BaseBackoff of retries {@link Duration}.
     *
     * @return The BaseBackoff {@link Duration}.
     */
    protected Duration getRetryBaseBackoff() {
        return _baseBackoff;
    }

    /**
     * Accessor for the MaximumDelay of retries {@link Duration}.
     *
     * @return The MaximumDelay {@link Duration}.
     */
    protected Duration getRetryMaximumDelay() {
        return _maximumDelay;
    }

    /**
     * Accessor for the status codes accepted as successful.
     *
     * @return the status codes accepted as success
     */
    ImmutableSet<Integer> getAcceptedStatusCodes() {
        return _acceptedStatusCodes;
    }

    /**
     * Accessor for the status codes that can be retried.
     *
     * @return the status codes that can be retried
     */
    ImmutableSet<Integer> getRetryableStatusCodes() {
        return _retryableStatusCodes;
    }

    /**
     * Serialize the {@link PeriodicData} instance for posting.
     *
     * @param periodicData The {@link PeriodicData} to be serialized.
     * @return The serialized representation of {@link PeriodicData}.
     */
    protected abstract Collection<SerializedDatum> serialize(PeriodicData periodicData);

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected HttpPostSink(final Builder<?, ?> builder) {
        super(builder);
        _uri = builder._uri;
        _aysncHttpClientUri = Uri.create(_uri.toString());

        _maximumAttempts = builder._maximumAttempts;
        _baseBackoff = builder._baseBackoff;
        _maximumDelay = builder._maximumDelay;
        _acceptedStatusCodes = builder._acceptedStatusCodes;
        _retryableStatusCodes = builder._retryableStatusCodes;

        _sinkActor = builder._actorSystem.actorOf(
                HttpSinkActor.props(
                        CLIENT,
                        this,
                        builder._maximumConcurrency,
                        builder._maximumQueueSize,
                        builder._spreadPeriod,
                        builder._periodicMetrics));
    }

    private final URI _uri;
    private final Uri _aysncHttpClientUri;
    private final ActorRef _sinkActor;
    private final int _maximumAttempts;
    private final Duration _baseBackoff;
    private final Duration _maximumDelay;
    private final ImmutableSet<Integer> _acceptedStatusCodes;
    private final ImmutableSet<Integer> _retryableStatusCodes;

    private static final Logger LOGGER = LoggerFactory.getLogger(HttpPostSink.class);
    private static final AsyncHttpClient CLIENT;

    static {
        final DefaultAsyncHttpClientConfig.Builder clientConfigBuilder = new DefaultAsyncHttpClientConfig.Builder();
        clientConfigBuilder.setThreadPoolName("HttpPostSinkWorker");
        final AsyncHttpClientConfig clientConfig = clientConfigBuilder.build();
        CLIENT = new DefaultAsyncHttpClient(clientConfig);
    }

    /**
     * Implementation of abstract builder pattern for {@link HttpPostSink}.
     *
     * @param <B> type of the builder
     * @param <S> type of the object to be built
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public abstract static class Builder<B extends BaseSink.Builder<B, S>, S extends HttpPostSink> extends BaseSink.Builder<B, S> {

        /**
         * The {@link URI} to post the aggregated data to. Cannot be null.
         *
         * @param value The {@link URI} to post the aggregated data to.
         * @return This instance of {@link Builder}.
         */
        public B setUri(final URI value) {
            _uri = value;
            return self();
        }

        /**
         * Sets the actor system to create the sink actor in. Required. Cannot be null. Injected by default.
         *
         * @param value the actor system
         * @return this builder
         */
        public B setActorSystem(final ActorSystem value) {
            _actorSystem = value;
            return self();
        }

        /**
         * Instance of {@link PeriodicMetrics}. Cannot be null. This field
         * may be injected automatically by Jackson/Guice if setup to do so.
         *
         * @param value Instance of {@link PeriodicMetrics}.
         * @return this builder
         */
        public B setPeriodicMetrics(final PeriodicMetrics value) {
            _periodicMetrics = value;
            return self();
        }

        /**
         * Sets the maximum concurrency of the http requests. Optional. Cannot be null.
         * Default is 1. Minimum is 1.
         *
         * @param value the maximum concurrency
         * @return this builder
         */
        public B setMaximumConcurrency(final Integer value) {
            _maximumConcurrency = value;
            return self();
        }

        /**
         * Sets the maximum delay before starting to send data to the server. Optional. Cannot be null.
         * Default is 0.
         *
         * @param value the maximum delay before sending new data
         * @return this builder
         */
        public B setSpreadPeriod(final Duration value) {
            _spreadPeriod = value;
            return self();
        }

        /**
         * Sets the maximum number of attempts of the http requests. Optional. Cannot be null.
         * Default is 1. Minimum is 1.
         *
         * @param value the maximum number of attempts of the http requests.
         * @return this builder
         */
        public B setMaximumAttempts(final Integer value) {
            _maximumAttempts = value;
            return self();
        }

        /**
         * Sets the maximum pending queue size. Optional Cannot be null.
         * Default is 25000. Minimum is 1.
         *
         * @param value the maximum pending queue size
         * @return this builder
         */
        public B setMaximumQueueSize(final Integer value) {
            _maximumQueueSize = value;
            return self();
        }

        /**
         * Sets the base backoff period. Optional Cannot be null.
         * Default is 50 milliseconds.
         *
         * @param value the base backoff period
         * @return this builder
         */
        public B setBaseBackoff(final Duration value) {
            _baseBackoff = value;
            return self();
        }

        /**
         * Sets the maximum delay for retries. Optional Cannot be null.
         * Default is 60 seconds.
         *
         * @param value the maximum delay for retries
         * @return this builder
         */
        public B setMaximumDelay(final Duration value) {
            _maximumDelay = value;
            return self();
        }

        /**
         * Sets the http status codes accepted as success. Optional. Cannot be null.
         * Default is: [200, 201, 202, 204]
         *
         * @param value the status codes accepted as success
         * @return this builder
         */
        public B setAcceptedStatusCodes(final ImmutableSet<Integer> value) {
            _acceptedStatusCodes = value;
            return self();
        }

        /**
         * Sets the http status codes that can be retried. Optional. Cannot be null.
         * Default is: [408, 429, 502, 503]
         *
         * @param value the status codes that can be retried
         * @return this builder
         */
        public B setRetryableStatusCodes(final ImmutableSet<Integer> value) {
            _retryableStatusCodes = value;
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
        private URI _uri;
        @JacksonInject
        @NotNull
        private ActorSystem _actorSystem;
        @NotNull
        @Min(1)
        private Integer _maximumConcurrency = 1;
        @NotNull
        @Min(1)
        private Integer _maximumQueueSize = 25000;
        @NotNull
        private Duration _spreadPeriod = Duration.ZERO;
        @JacksonInject
        @NotNull
        private PeriodicMetrics _periodicMetrics;
        @NotNull
        @Min(1)
        private Integer _maximumAttempts = 1;
        @NotNull
        private Duration _baseBackoff = Duration.ofMillis(50);
        @NotNull
        @CheckWith(CheckPeriod.class)
        private Duration _maximumDelay = Duration.ofSeconds(60);
        @NotNull
        private ImmutableSet<Integer> _acceptedStatusCodes = DEFAULT_ACCEPTED_STATUS_CODES;
        @NotNull
        private ImmutableSet<Integer> _retryableStatusCodes = DEFAULT_RETRYABLE_STATUS_CODES;

        private static final ImmutableSet<Integer> DEFAULT_ACCEPTED_STATUS_CODES;
        private static final ImmutableSet<Integer> DEFAULT_RETRYABLE_STATUS_CODES;

        static {
            DEFAULT_ACCEPTED_STATUS_CODES = ImmutableSet.<Integer>builder()
                    .add(StatusCodes.OK.intValue())
                    .add(StatusCodes.CREATED.intValue())
                    .add(StatusCodes.ACCEPTED.intValue())
                    .add(StatusCodes.NO_CONTENT.intValue())
                    .build();
        }

        static {
            DEFAULT_RETRYABLE_STATUS_CODES = ImmutableSet.<Integer>builder()
                    .add(StatusCodes.REQUEST_TIMEOUT.intValue())
                    .add(StatusCodes.TOO_MANY_REQUESTS.intValue())
                    .add(StatusCodes.BAD_GATEWAY.intValue())
                    .add(StatusCodes.SERVICE_UNAVAILABLE.intValue())
                    .build();
        }

        private static final class CheckPeriod implements CheckWithCheck.SimpleCheck {

            @Serial
            private static final long serialVersionUID = -6924010227680984149L;

            @Override
            public boolean isSatisfied(
                    final Object validatedObject,
                    final Object value,
                    final OValContext context,
                    final Validator validator) {
                if (!(validatedObject instanceof HttpPostSink.Builder)) {
                    return false;
                }
                final HttpPostSink.Builder<?, ?> builder = (HttpPostSink.Builder<?, ?>) validatedObject;
                return builder._baseBackoff.toMillis() >= 0 && builder._maximumDelay.toMillis() >= 0;
            }
        }
    }

    static final class SerializedDatum {
        SerializedDatum(final byte[] datum, final Optional<Long> populationSize) {
            _datum = datum;
            _populationSize = populationSize;
        }

        public byte[] getDatum() {
            return _datum;
        }

        public Optional<Long> getPopulationSize() {
            return _populationSize;
        }

        private final byte[] _datum;
        private final Optional<Long> _populationSize;
    }
}
