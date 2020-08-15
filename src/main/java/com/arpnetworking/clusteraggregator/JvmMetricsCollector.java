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
package com.arpnetworking.clusteraggregator;

import akka.actor.AbstractActor;
import akka.actor.Cancellable;
import akka.actor.Props;
import akka.actor.Scheduler;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.jvm.JvmMetricsRunnable;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.base.MoreObjects;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * Actor responsible for collecting JVM metrics on a periodic basis.
 *
 * @author Deepika Misra (deepika at groupon dot com)
 */
public final class JvmMetricsCollector extends AbstractActor {

    /**
     * Creates a {@link Props} for construction in Akka.
     *
     * @param interval An instance of {@link Duration}.
     * @param metricsFactory A {@link MetricsFactory} to use for metrics creation.
     * @return A new {@link Props}.
     */
    public static Props props(
            final Duration interval,
            final MetricsFactory metricsFactory) {
        return Props.create(JvmMetricsCollector.class, interval, metricsFactory);
    }

    @Override
    public void preStart() {
        LOGGER.info()
                .setMessage("Starting JVM metrics collector actor.")
                .addData("actor", self().toString())
                .log();
        _cancellable = _scheduler.schedule(
                INITIAL_DELAY,
                _interval,
                self(),
                new CollectJvmMetrics(),
                getContext().system().dispatcher(),
                self());
    }

    @Override
    public void postStop() {
        LOGGER.info().setMessage("Stopping JVM metrics collection.").log();
        _cancellable.cancel();
    }

    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(CollectJvmMetrics.class, message -> {
                    LOGGER.trace().setMessage("Message received")
                            .addData("data", message)
                            .addData("actor", self().toString())
                            .log();
                    _jvmMetricsRunnable.run();
                })
                .build();
    }

    /* package private */ Cancellable getCancellable() {
        return _cancellable;
    }

    /**
     * Package private constructor. Strictly for testing.
     *
     * @param interval An instance of {@link FiniteDuration}.
     * @param runnable An instance of {@link Runnable}.
     */
    /* package private */ JvmMetricsCollector(
            final FiniteDuration interval,
            final Runnable runnable,
            @Nullable final Scheduler scheduler) {
        _interval = interval;
        _jvmMetricsRunnable = runnable;
        if (scheduler == null) {
            _scheduler = getContext().system().scheduler();
        } else {
            _scheduler = scheduler;
        }
    }

    private JvmMetricsCollector(
            final Duration interval,
            final MetricsFactory metricsFactory) {
        this(
                FiniteDuration.create(
                        interval.toMillis(),
                        TimeUnit.MILLISECONDS),
                new JvmMetricsRunnable.Builder()
                        .setMetricsFactory(metricsFactory)
                        .setSwallowException(false) // Relying on the default akka supervisor strategy here.
                        .build(),
        null
        );
    }
    private Cancellable _cancellable;

    private final FiniteDuration _interval;
    private final Runnable _jvmMetricsRunnable;
    private final Scheduler _scheduler;

    private static final FiniteDuration INITIAL_DELAY = FiniteDuration.Zero();
    private static final Logger LOGGER = LoggerFactory.getLogger(JvmMetricsCollector.class);

    /**
     * Message class to collect JVM metrics. Package private for testing.
     *
     * @author Deepika Misra (deepika at groupon dot com)
     */
    /* package private */ static final class CollectJvmMetrics {

        @Override
        public String toString() {
            return MoreObjects.toStringHelper(this)
                    .add("id", Integer.toHexString(System.identityHashCode(this)))
                    .toString();
        }

        /* package private */ CollectJvmMetrics() { }
    }
}
