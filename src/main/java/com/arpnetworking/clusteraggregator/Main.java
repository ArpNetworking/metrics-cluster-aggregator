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

import ch.qos.logback.classic.LoggerContext;
import com.arpnetworking.clusteraggregator.configuration.ClusterAggregatorConfiguration;
import com.arpnetworking.commons.builder.Builder;
import com.arpnetworking.configuration.jackson.DynamicConfiguration;
import com.arpnetworking.configuration.jackson.HoconFileSource;
import com.arpnetworking.configuration.jackson.JsonNodeFileSource;
import com.arpnetworking.configuration.jackson.JsonNodeSource;
import com.arpnetworking.configuration.triggers.FileTrigger;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.utility.Configurator;
import com.arpnetworking.utility.Database;
import com.arpnetworking.utility.Launchable;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;
import com.google.inject.Guice;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.TypeLiteral;
import com.google.inject.name.Names;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.pattern.Patterns;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.Optional;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * Entry point for the pekko-based cluster aggregator.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class Main implements Launchable {
    /**
     * Entry point.
     *
     * @param args command line arguments
     */
    public static void main(final String[] args) {
        Thread.setDefaultUncaughtExceptionHandler(
                (thread, throwable) -> {
                    LOGGER.error()
                            .setMessage("Unhandled exception!")
                            .setThrowable(throwable)
                            .log();
                });

        Thread.currentThread().setUncaughtExceptionHandler(
                (thread, throwable) -> {
                    LOGGER.error()
                            .setMessage("Unhandled exception!")
                            .setThrowable(throwable)
                            .log();
                }
        );

        LOGGER.info()
                .setMessage("Launching cluster-aggregator")
                .log();

        Runtime.getRuntime().addShutdownHook(SHUTDOWN_THREAD);

        if (args.length != 1) {
            throw new RuntimeException("No configuration file specified");
        }

        LOGGER.debug()
                .setMessage("Loading configuration from file")
                .addData("file", args[0])
                .log();

        Optional<DynamicConfiguration> configuration = Optional.empty();
        Optional<Configurator<Main, ClusterAggregatorConfiguration>> configurator = Optional.empty();
        try {
            final File configurationFile = new File(args[0]);
            configurator = Optional.of(new Configurator<>(Main::new, ClusterAggregatorConfiguration.class));
            final ObjectMapper objectMapper = ClusterAggregatorConfiguration.createObjectMapper();
            configuration = Optional.of(new DynamicConfiguration.Builder()
                    .setObjectMapper(objectMapper)
                    .addSourceBuilder(getFileSourceBuilder(configurationFile, objectMapper))
                    .addTrigger(new FileTrigger.Builder().setFile(configurationFile).build())
                    .addListener(configurator.get())
                    .build());

            configuration.get().launch();

            // Wait for application shutdown
            LOGGER.info()
                    .setMessage("Configurators launched, waiting for shutdown signal")
                    .log();
            SHUTDOWN_SEMAPHORE.acquire();
            LOGGER.info()
                    .setMessage("Shutdown signal received, shutting down main thread")
                    .log();
        } catch (final InterruptedException e) {
            throw new RuntimeException(e);
        } finally {
            if (configurator.isPresent()) {
                configurator.get().shutdown();
            } else {
                LOGGER.warn()
                        .setMessage("No configurator present to shut down")
                        .log();
            }
            if (configuration.isPresent()) {
                configuration.get().shutdown();
            } else {
                LOGGER.warn()
                        .setMessage("No running configuration present to shut down")
                        .log();
            }
            // Notify the shutdown that we're done
            SHUTDOWN_SEMAPHORE.release();
        }
    }

    /**
     * Public constructor.
     *
     * @param configuration The configuration object.
     */
    public Main(final ClusterAggregatorConfiguration configuration) {
        _configuration = configuration;
    }

    /**
     * Launch the component.
     */
    @Override
    public synchronized void launch() {
        final Injector injector = launchGuice();
        launchDatabases(injector);
        launchPekko(injector);
        launchActors(injector);
    }

    /**
     * Shutdown the component.
     */
    @Override
    public synchronized void shutdown() {
        shutdownPekko();
        shutdownDatabases();
        shutdownGuice();
    }

    private Injector launchGuice() {
        _guiceAppShutdown = new AppShutdown();
        return Guice.createInjector(new GuiceModule(_configuration, _guiceAppShutdown));
    }

    private void launchDatabases(final Injector injector) {
        _databases = Lists.newArrayList();
        for (final String databaseName : _configuration.getDatabaseConfigurations().keySet()) {
            final Database database = injector.getInstance(Key.get(Database.class, Names.named(databaseName)));
            _databases.add(database);
            LOGGER.info()
                    .setMessage("Launching database")
                    .addData("database", database)
                    .log();
            database.launch();
        }
    }

    private void launchActors(final Injector injector) {
        injector.getInstance(Key.get(ActorRef.class, Names.named("host-emitter")));
        injector.getInstance(Key.get(ActorRef.class, Names.named("cluster-emitter")));

        injector.getInstance(Key.get(ActorRef.class, Names.named("periodic-statistics")));

        LOGGER.info()
                .setMessage("Launching shard region")
                .log();
        injector.getInstance(Key.get(ActorRef.class, Names.named("aggregator-shard-region")));

        LOGGER.info()
                .setMessage("Launching tcp server")
                .log();
        injector.getInstance(Key.get(ActorRef.class, Names.named("tcp-server")));
        injector.getInstance(Key.get(ActorRef.class, Names.named("status-cache")));

        LOGGER.info()
                .setMessage("Launching JVM metrics collector")
                .log();
        injector.getInstance(Key.get(ActorRef.class, Names.named("jvm-metrics-collector")));

        LOGGER.info()
                .setMessage("Launching http server")
                .log();
        injector.getInstance(
                Key.get(
                        SOURCE_TYPE_LITERAL,
                        Names.named("http-server")));

        LOGGER.info()
                .setMessage("Launching graceful shutdown actor")
                .log();
        _shutdownActor = injector.getInstance(Key.get(ActorRef.class, Names.named("graceful-shutdown-actor")));
    }

    private void launchPekko(final Injector injector) {
        LOGGER.info()
                .setMessage("Launching Pekko")
                .log();
        _system = injector.getInstance(ActorSystem.class);
    }

    private void shutdownGuice() {
        LOGGER.info().setMessage("Stopping guice").log();
        if (_guiceAppShutdown != null) {
            _guiceAppShutdown.shutdown();
        }
    }

    private void shutdownDatabases() {
        for (final Database database : _databases) {
            LOGGER.info()
                    .setMessage("Stopping database")
                    .addData("database", database)
                    .log();
            database.shutdown();
        }
    }

    private void shutdownPekko() {
        LOGGER.info()
                .setMessage("Stopping Pekko")
                .log();

        try {
            if (_shutdownActor != null) {
                final CompletionStage<Object> gracefulShutdown =
                        Patterns.ask(_shutdownActor, GracefulShutdownActor.Shutdown.getInstance(), Duration.ofMinutes(10));
                gracefulShutdown.toCompletableFuture().join();
                LOGGER.info()
                        .setMessage("Graceful shutdown actor reported completion")
                        .log();
            }
            if (_system != null) {
                _system.getWhenTerminated().toCompletableFuture().get(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
            }
            // CHECKSTYLE.OFF: IllegalCatch - Prevent program shutdown
        } catch (final InterruptedException | TimeoutException | ExecutionException e) {
            // CHECKSTYLE.ON: IllegalCatch
            LOGGER.warn()
                    .setMessage("Interrupted at shutdown")
                    .setThrowable(e)
                    .log();
        }
    }

    private static Builder<? extends JsonNodeSource> getFileSourceBuilder(
            final File configurationFile,
            final ObjectMapper objectMapper) {
        if (configurationFile.getName().toLowerCase(Locale.getDefault()).endsWith(HOCON_FILE_EXTENSION)) {
            return new HoconFileSource.Builder()
                .setObjectMapper(objectMapper)
                .setFile(configurationFile);
        }
        return new JsonNodeFileSource.Builder()
                .setObjectMapper(objectMapper)
                .setFile(configurationFile);
    }

    private final ClusterAggregatorConfiguration _configuration;

    private volatile ActorSystem _system;
    private volatile ActorRef _shutdownActor;
    private volatile AppShutdown _guiceAppShutdown;
    private volatile List<Database> _databases;

    private static final Logger LOGGER = com.arpnetworking.steno.LoggerFactory.getLogger(Main.class);
    private static final Duration SHUTDOWN_TIMEOUT = Duration.ofMinutes(3);
    private static final SourceTypeLiteral SOURCE_TYPE_LITERAL = new SourceTypeLiteral();
    private static final Semaphore SHUTDOWN_SEMAPHORE = new Semaphore(0, true);
    private static final Thread SHUTDOWN_THREAD = new ShutdownThread();
    private static final String HOCON_FILE_EXTENSION = ".conf";

    private static final class ShutdownThread extends Thread {
        private ShutdownThread() {
            super("ClusterAggregatorShutdownHook");
        }

        @Override
        public void run() {
            LOGGER.info()
                    .setMessage("Stopping cluster-aggregator")
                    .log();

            // release the main thread waiting for shutdown signal
            SHUTDOWN_SEMAPHORE.release();

            try {
                // wait for it to signal that it has completed shutdown
                if (!SHUTDOWN_SEMAPHORE.tryAcquire(SHUTDOWN_TIMEOUT.toSeconds(), TimeUnit.SECONDS)) {
                    LOGGER.warn()
                            .setMessage("Shutdown did not complete in a timely manner")
                            .log();
                }
            } catch (final InterruptedException e) {
                throw new RuntimeException(e);
            } finally {
                LOGGER.info()
                        .setMessage("Shutdown complete")
                        .log();
                final LoggerContext context = (LoggerContext) LoggerFactory.getILoggerFactory();
                context.stop();
            }
        }
    }

    private static final class SourceTypeLiteral extends TypeLiteral<CompletionStage<ServerBinding>> {}
}
