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

import com.arpnetworking.clusteraggregator.aggregation.AggMessageExtractor;
import com.arpnetworking.clusteraggregator.aggregation.AggregationRouter;
import com.arpnetworking.clusteraggregator.client.AggClientServer;
import com.arpnetworking.clusteraggregator.client.AggClientSupervisor;
import com.arpnetworking.clusteraggregator.client.HttpSourceActor;
import com.arpnetworking.clusteraggregator.configuration.ClusterAggregatorConfiguration;
import com.arpnetworking.clusteraggregator.configuration.ConfigurableActorProxy;
import com.arpnetworking.clusteraggregator.configuration.DatabaseConfiguration;
import com.arpnetworking.clusteraggregator.configuration.EmitterConfiguration;
import com.arpnetworking.clusteraggregator.configuration.RebalanceConfiguration;
import com.arpnetworking.clusteraggregator.http.Routes;
import com.arpnetworking.clusteraggregator.partitioning.DatabasePartitionSet;
import com.arpnetworking.commons.builder.Builder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.configuration.jackson.DynamicConfiguration;
import com.arpnetworking.configuration.jackson.HoconFileSource;
import com.arpnetworking.configuration.jackson.JsonNodeFileSource;
import com.arpnetworking.configuration.jackson.JsonNodeSource;
import com.arpnetworking.configuration.triggers.FileTrigger;
import com.arpnetworking.guice.akka.GuiceActorCreator;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.metrics.Sink;
import com.arpnetworking.metrics.impl.ApacheHttpSink;
import com.arpnetworking.metrics.impl.TsdMetricsFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.metrics.incubator.impl.TsdPeriodicMetrics;
import com.arpnetworking.utility.ActorConfigurator;
import com.arpnetworking.utility.ConfiguredLaunchableFactory;
import com.arpnetworking.utility.Database;
import com.arpnetworking.utility.ParallelLeastShardAllocationStrategy;
import com.arpnetworking.utility.partitioning.PartitionSet;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Key;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.cluster.sharding.ClusterSharding;
import org.apache.pekko.cluster.sharding.ClusterShardingSettings;
import org.apache.pekko.http.javadsl.Http;
import org.apache.pekko.http.javadsl.IncomingConnection;
import org.apache.pekko.http.javadsl.ServerBinding;
import org.apache.pekko.routing.DefaultResizer;
import org.apache.pekko.routing.RoundRobinPool;
import org.apache.pekko.stream.Materializer;
import org.apache.pekko.stream.javadsl.Source;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import javax.annotation.Nullable;

/**
 * The primary Guice module used to bootstrap the cluster aggregator. NOTE: this module will be constructed whenever
 * a new configuration is loaded, and will be torn down when another configuration is loaded.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class GuiceModule extends AbstractModule {
    /**
     * Public constructor.
     *
     * @param configuration The configuration.
     * @param shutdown The shutdown hook.
     */
    public GuiceModule(final ClusterAggregatorConfiguration configuration, final LifecycleRegistration shutdown) {
        _configuration = configuration;
        _shutdown = shutdown;
    }

    @Override
    protected void configure() {
        bind(ClusterAggregatorConfiguration.class).toInstance(_configuration);
        bind(LifecycleRegistration.class).toInstance(_shutdown);

        for (final Map.Entry<String, DatabaseConfiguration> entry : _configuration.getDatabaseConfigurations().entrySet()) {
            bind(Database.class)
                    .annotatedWith(Names.named(entry.getKey()))
                    .toProvider(new DatabaseProvider(entry.getKey(), entry.getValue()))
                    .in(Singleton.class);
        }

        bind(String.class).annotatedWith(Names.named("health-check-path")).toInstance(_configuration.getHttpHealthCheckPath());
        bind(String.class).annotatedWith(Names.named("status-path")).toInstance(_configuration.getHttpStatusPath());
        bind(String.class).annotatedWith(Names.named("version-path")).toInstance(_configuration.getHttpVersionPath());
        bind(ActorRef.class)
                .annotatedWith(Names.named("http-ingest-v1"))
                .toProvider(GuiceActorCreator.provider(HttpSourceActor.class, "http-ingest-v1"))
                .asEagerSingleton();
    }

    @Provides
    @Singleton
    @Named("akka-config")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private Config provideAkkaConfig() {
        // This is necessary because the keys contain periods which when
        // transforming from a map are considered compound path elements. By
        // rendering to JSON and then parsing it this forces the keys to be
        // quoted and thus considered single path elements even with periods.
        try {
            final String akkaJsonConfig = OBJECT_MAPPER.writeValueAsString(_configuration.getAkkaConfiguration());
            return ConfigFactory.parseString(
                    akkaJsonConfig,
                    ConfigParseOptions.defaults()
                            .setSyntax(ConfigSyntax.JSON));
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Provides
    @Singleton
    @SuppressWarnings("deprecation")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private MetricsFactory provideMetricsFactory() throws URISyntaxException {
        final ImmutableList.Builder<com.arpnetworking.metrics.Sink> monitoringSinksBuilder =
                new ImmutableList.Builder<>();
        if (_configuration.getMonitoringHost().isPresent()
                || _configuration.getMonitoringPort().isPresent()) {
            final String endpoint = String.format(
                    "http://%s:%d/metrics/v3/application",
                    _configuration.getMonitoringHost().orElse("localhost"),
                    _configuration.getMonitoringPort().orElse(7090));

            monitoringSinksBuilder.add(
                    new ApacheHttpSink.Builder()
                            .setUri(URI.create(endpoint))
                            .build());
        } else {
            monitoringSinksBuilder.addAll(createSinks(_configuration.getMonitoringSinks()));
        }

        return new TsdMetricsFactory.Builder()
                .setClusterName(_configuration.getMonitoringCluster())
                .setServiceName(_configuration.getMonitoringService())
                .setSinks(monitoringSinksBuilder.build())
                .build();
    }

    @Provides
    @Singleton
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorSystem provideActorSystem(@Named("akka-config") final Config akkaConfig) {
        System.out.println(akkaConfig);
        return ActorSystem.create("Metrics", akkaConfig);
    }

    @Provides
    @Singleton
    @Named("cluster-emitter")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideClusterEmitter(final Injector injector, final ActorSystem system) {
        return launchEmitter(injector, system, _configuration.getClusterPipelineConfiguration(), "cluster-emitter-configurator");
    }

    @Provides
    @Singleton
    @Named("host-emitter")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideHostEmitter(final Injector injector, final ActorSystem system) {
        return launchEmitter(injector, system, _configuration.getHostPipelineConfiguration(), "host-emitter-configurator");
    }

    private ActorRef launchEmitter(final Injector injector, final ActorSystem system, final File pipelineFile, final String name) {
        final ActorRef emitterConfigurationProxy = system.actorOf(
                ConfigurableActorProxy.props(new RoundRobinEmitterFactory()),
                name);
        final ActorConfigurator<EmitterConfiguration> configurator =
                new ActorConfigurator<>(emitterConfigurationProxy, EmitterConfiguration.class);
        final ObjectMapper objectMapper = EmitterConfiguration.createObjectMapper(injector);
        final Builder<? extends JsonNodeSource> sourceBuilder;
        if (pipelineFile.getName().toLowerCase(Locale.getDefault()).endsWith(HOCON_FILE_EXTENSION)) {
            sourceBuilder = new HoconFileSource.Builder()
                    .setObjectMapper(objectMapper)
                    .setFile(pipelineFile);
        } else {
            sourceBuilder = new JsonNodeFileSource.Builder()
                    .setObjectMapper(objectMapper)
                    .setFile(pipelineFile);
        }

        final DynamicConfiguration configuration = new DynamicConfiguration.Builder()
                .setObjectMapper(objectMapper)
                .addSourceBuilder(sourceBuilder)
                .addTrigger(new FileTrigger.Builder().setFile(pipelineFile).build())
                .addListener(configurator)
                .build();

        configuration.launch();

        return emitterConfigurationProxy;
    }

    @Provides
    @Singleton
    @Named("status-cache")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideStatusCache(
            final ActorSystem system,
            @Named("periodic-statistics") final ActorRef periodicStats,
            final MetricsFactory metricsFactory) {
        final Cluster cluster = Cluster.get(system);
        final ActorRef clusterStatusCache = system.actorOf(
                ClusterStatusCache.props(
                        system,
                        _configuration.getClusterStatusInterval(),
                        metricsFactory),
                "cluster-status");
        return system.actorOf(Status.props(cluster, clusterStatusCache, periodicStats), "status");
    }

    @Provides
    @Singleton
    @Named("tcp-server")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideTcpServer(final Injector injector, final ActorSystem system) {
        return system.actorOf(GuiceActorCreator.props(injector, AggClientServer.class), "tcp-server");
    }

    @Provides
    @Singleton
    @Named("http-server")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private java.util.concurrent.CompletionStage<akka.http.javadsl.ServerBinding> provideHttpServer(
            final ActorSystem system,
            final Routes routes) {
        // Create and bind Http server
        final Materializer materializer = Materializer.createMaterializer(system);
        final Http http = Http.get(system);
        final Source<IncomingConnection, CompletionStage<ServerBinding>> binding = http.newServerAt(
                        _configuration.getHttpHost(),
                        _configuration.getHttpPort())
                .connectionSource();
        return binding.to(
                akka.stream.javadsl.Sink.foreach(
                        connection -> connection.handleWithAsyncHandler(routes, materializer)))
                .run(materializer);
    }

    @Provides
    @Singleton
    @Named("periodic-statistics")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef providePeriodicStatsActor(final ActorSystem system, final MetricsFactory metricsFactory) {
        return system.actorOf(PeriodicStatisticsActor.props(metricsFactory));
    }

    @Provides
    @Singleton
    @Named("aggregator-shard-region")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideAggregatorShardRegion(
            final ActorSystem system,
            final Injector injector,
            final AggMessageExtractor extractor) {
        final ClusterSharding clusterSharding = ClusterSharding.get(system);
        final RebalanceConfiguration rebalanceConfiguration = _configuration.getRebalanceConfiguration();

        final ClusterShardingSettings settings = ClusterShardingSettings.create(system);

        return clusterSharding.start(
                "Aggregator",
                GuiceActorCreator.props(injector, AggregationRouter.class),
                settings,
                extractor,
                new ParallelLeastShardAllocationStrategy(
                        rebalanceConfiguration.getMaxParallel(),
                        rebalanceConfiguration.getThreshold(),
                        Optional.of(system.actorSelection("/user/cluster-status"))),
                new AggregationRouter.ShutdownAggregator());
    }

    @Provides
    @Singleton
    @Named("jvm-metrics-collector")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideJvmMetricsCollector(final ActorSystem system, final MetricsFactory metricsFactory) {
        return system.actorOf(JvmMetricsCollector.props(_configuration.getJvmMetricsCollectionInterval(), metricsFactory));
    }

    @Provides
    @Singleton
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private PeriodicMetrics providePeriodicMetrics(final MetricsFactory metricsFactory, final LifecycleRegistration lifecycle) {
        final TsdPeriodicMetrics periodicMetrics = new TsdPeriodicMetrics.Builder()
                .setMetricsFactory(metricsFactory)
                .build();
        final ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor(
                r -> new Thread(r, "PeriodicMetricsCloser"));
        final long offsetMillis = 1250 - (System.currentTimeMillis() % 1000);
        executor.scheduleAtFixedRate(periodicMetrics, offsetMillis, 1000, TimeUnit.MILLISECONDS);
        lifecycle.registerShutdown(() -> {
            executor.shutdown();
            return CompletableFuture.completedFuture(null);
        });
        return periodicMetrics;
    }

    @Provides
    @Singleton
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private AggMessageExtractor provideExtractor(
            @Named("reaggregation-dimensions") final ImmutableSet<String> reaggregationDimensions) {
        return new AggMessageExtractor(reaggregationDimensions);
    }

    @Provides
    @Named("agg-client-supervisor")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private Props provideAggClientSupervisorProvider(final Injector injector) {
        return GuiceActorCreator.props(injector, AggClientSupervisor.class);
    }

    @Provides
    @Singleton
    @Named("graceful-shutdown-actor")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideGracefulShutdownActor(final ActorSystem system, final Injector injector) {
        return system.actorOf(GuiceActorCreator.props(injector, GracefulShutdownActor.class), "graceful-shutdown");
    }

    @Provides
    @Named("cluster-host-suffix")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private String provideClusterHostSuffix(final ClusterAggregatorConfiguration config) {
        return config.getClusterHostSuffix();
    }

    @Provides
    @Named("reaggregation-dimensions")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ImmutableSet<String> provideReaggregationDimensions(final ClusterAggregatorConfiguration config) {
        return config.getReaggregationDimensions();
    }

    @Provides
    @Named("reaggregation-cluster-as-host")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private boolean provideReaggregationInjectClusterAsHost(final ClusterAggregatorConfiguration config) {
        return config.getReaggregationInjectClusterAsHost();
    }

    @Provides
    @Named("reaggregation-timeout")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private Duration provideReaggregationTimeout(final ClusterAggregatorConfiguration config) {
        return config.getReaggregationTimeout();
    }

    @Named("circonus-partition-set")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private PartitionSet provideDatabasePartitionSet(final Injector injector) {
        final Database database = injector.getInstance(Key.get(Database.class, Names.named("metrics_clusteragg")));
        final com.arpnetworking.clusteraggregator.models.ebean.PartitionSet partitionSet =
                com.arpnetworking.clusteraggregator.models.ebean.PartitionSet.findOrCreate(
                        "circonus-partition-set",
                        database,
                        1000,
                        Integer.MAX_VALUE);
        return new DatabasePartitionSet(database, partitionSet);
    }

    @SuppressFBWarnings("REC_CATCH_EXCEPTION")
    static List<Sink> createSinks(final ImmutableList<JsonNode> monitoringSinks) {
        // Until we implement the Commons Builder pattern in the metrics client
        // library we need to resort to a more brute-force deserialization
        // style. The benefit of this approach is that it will be forwards
        // compatible with the Commons Builder approach. The drawbacks are
        // the ugly way the configuration is passed around (as JsonNode) and
        // then two-step deserialized.
        final List<com.arpnetworking.metrics.Sink> sinks = new ArrayList<>();
        for (final JsonNode sinkNode : monitoringSinks) {
            @Nullable final JsonNode classNode = sinkNode.get("class");
            try {
                if (classNode != null) {
                    final Class<?> builderClass = Class.forName(classNode.textValue() + "$Builder");
                    final Object builder = OBJECT_MAPPER.treeToValue(sinkNode, builderClass);
                    @SuppressWarnings("unchecked")
                    final com.arpnetworking.metrics.Sink sink =
                            (com.arpnetworking.metrics.Sink) builderClass.getMethod("build").invoke(builder);
                    sinks.add(sink);
                }
                // CHECKSTYLE.OFF: IllegalCatch - There are so many ways this hack can fail!
            } catch (final Exception e) {
                // CHECKSTYLE.ON: IllegalCatch
                throw new RuntimeException("Unable to create sink from: " + sinkNode.toString(), e);
            }
        }
        return sinks;
    }

    private final ClusterAggregatorConfiguration _configuration;
    private final LifecycleRegistration _shutdown;

    private static final String HOCON_FILE_EXTENSION = ".conf";
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    private static final class RoundRobinEmitterFactory implements ConfiguredLaunchableFactory<Props, EmitterConfiguration> {

        @Override
        public Props create(final EmitterConfiguration config) {
            final DefaultResizer resizer = new DefaultResizer(config.getPoolSize(), config.getPoolSize());
            return new RoundRobinPool(config.getPoolSize()).withResizer(resizer).props(Emitter.props(config));
        }
    }

    private static final class DatabaseProvider implements com.google.inject.Provider<Database> {

        private DatabaseProvider(final String name, final DatabaseConfiguration configuration) {
            _name = name;
            _configuration = configuration;
        }

        @Override
        public Database get() {
            return new Database(_name, _configuration);
        }

        private final String _name;
        private final DatabaseConfiguration _configuration;
    }
}
