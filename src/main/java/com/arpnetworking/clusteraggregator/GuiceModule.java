/**
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

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.PoisonPill;
import akka.actor.Props;
import akka.cluster.Cluster;
import akka.cluster.sharding.ClusterSharding;
import akka.cluster.sharding.ClusterShardingSettings;
import akka.cluster.singleton.ClusterSingletonManager;
import akka.cluster.singleton.ClusterSingletonManagerSettings;
import akka.cluster.singleton.ClusterSingletonProxy;
import akka.cluster.singleton.ClusterSingletonProxySettings;
import akka.http.javadsl.ConnectHttp;
import akka.http.javadsl.Http;
import akka.http.javadsl.IncomingConnection;
import akka.http.javadsl.ServerBinding;
import akka.routing.DefaultResizer;
import akka.routing.RoundRobinPool;
import akka.stream.ActorMaterializer;
import akka.stream.Materializer;
import com.arpnetworking.clusteraggregator.aggregation.AggMessageExtractor;
import com.arpnetworking.clusteraggregator.aggregation.AggregationRouter;
import com.arpnetworking.clusteraggregator.aggregation.Bookkeeper;
import com.arpnetworking.clusteraggregator.bookkeeper.persistence.InMemoryBookkeeper;
import com.arpnetworking.clusteraggregator.client.AggClientServer;
import com.arpnetworking.clusteraggregator.client.AggClientSupervisor;
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
import com.arpnetworking.metrics.impl.TsdLogSink;
import com.arpnetworking.metrics.impl.TsdMetricsFactory;
import com.arpnetworking.utility.ActorConfigurator;
import com.arpnetworking.utility.ConfiguredLaunchableFactory;
import com.arpnetworking.utility.Database;
import com.arpnetworking.utility.ParallelLeastShardAllocationStrategy;
import com.arpnetworking.utility.partitioning.PartitionSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.inject.AbstractModule;
import com.google.inject.Injector;
import com.google.inject.Provides;
import com.google.inject.Singleton;
import com.google.inject.name.Named;
import com.google.inject.name.Names;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigParseOptions;
import com.typesafe.config.ConfigSyntax;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletionStage;

/**
 * The primary Guice module used to bootstrap the cluster aggregator. NOTE: this module will be constructed whenever
 * a new configuration is loaded, and will be torn down when another configuration is loaded.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public class GuiceModule extends AbstractModule {
    /**
     * Public constructor.
     *
     * @param configuration The configuration.
     */
    public GuiceModule(final ClusterAggregatorConfiguration configuration) {
        _configuration = configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    protected void configure() {
        bind(ClusterAggregatorConfiguration.class).toInstance(_configuration);

        for (final Map.Entry<String, DatabaseConfiguration> entry : _configuration.getDatabaseConfigurations().entrySet()) {
            bind(Database.class)
                    .annotatedWith(Names.named(entry.getKey()))
                    .toProvider(new DatabaseProvider(entry.getKey(), entry.getValue()))
                    .in(Singleton.class);
        }
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
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private MetricsFactory provideMetricsFactory() {
        final Sink sink = new TsdLogSink.Builder()
                .setName("cluster-aggregator-query")
                .setDirectory(_configuration.getLogDirectory())
                .build();

        return new TsdMetricsFactory.Builder()
                .setClusterName(_configuration.getMonitoringCluster())
                .setServiceName("cluster_aggregator")
                .setSinks(Collections.singletonList(sink))
                .build();
    }

    @Provides
    @Singleton
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorSystem provideActorSystem(@Named("akka-config") final Config akkaConfig) {
        return ActorSystem.create("Metrics", akkaConfig);
    }

    @Provides
    @Singleton
    @Named("cluster-emitter")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideClusterEmitter(final Injector injector, final ActorSystem system) {
        final ActorRef emitterConfigurationProxy = system.actorOf(
                ConfigurableActorProxy.props(new RoundRobinEmitterFactory()),
                "cluster-emitter-configurator");
        final ActorConfigurator<EmitterConfiguration> configurator =
                new ActorConfigurator<>(emitterConfigurationProxy, EmitterConfiguration.class);
        final ObjectMapper objectMapper = EmitterConfiguration.createObjectMapper(injector);
        final File configurationFile = _configuration.getClusterPipelineConfiguration();
        final Builder<? extends JsonNodeSource> sourceBuilder;
        if (configurationFile.getName().toLowerCase(Locale.getDefault()).endsWith(HOCON_FILE_EXTENSION)) {
            sourceBuilder = new HoconFileSource.Builder()
                    .setObjectMapper(objectMapper)
                    .setFile(configurationFile);
        } else {
            sourceBuilder = new JsonNodeFileSource.Builder()
                    .setObjectMapper(objectMapper)
                    .setFile(configurationFile);
        }

        final DynamicConfiguration configuration = new DynamicConfiguration.Builder()
                .setObjectMapper(objectMapper)
                .addSourceBuilder(sourceBuilder)
                .addTrigger(new FileTrigger.Builder().setFile(_configuration.getClusterPipelineConfiguration()).build())
                .addListener(configurator)
                .build();

        configuration.launch();

        return emitterConfigurationProxy;
    }

    @Provides
    @Singleton
    @Named("host-emitter")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideHostEmitter(final Injector injector, final ActorSystem system) {
        final ActorRef emitterConfigurationProxy = system.actorOf(
                ConfigurableActorProxy.props(new RoundRobinEmitterFactory()),
                "host-emitter-configurator");
        final ActorConfigurator<EmitterConfiguration> configurator =
                new ActorConfigurator<>(emitterConfigurationProxy, EmitterConfiguration.class);
        final ObjectMapper objectMapper = EmitterConfiguration.createObjectMapper(injector);
        final DynamicConfiguration configuration = new DynamicConfiguration.Builder()
                .setObjectMapper(objectMapper)
                .addSourceBuilder(
                        new JsonNodeFileSource.Builder()
                                .setObjectMapper(objectMapper)
                                .setFile(_configuration.getHostPipelineConfiguration()))
                .addTrigger(new FileTrigger.Builder().setFile(_configuration.getHostPipelineConfiguration()).build())
                .addListener(configurator)
                .build();

        configuration.launch();

        return emitterConfigurationProxy;
    }

    @Provides
    @Singleton
    @Named("bookkeeper-proxy")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideBookkeeperProxy(final ActorSystem system) {
        system.actorOf(
                ClusterSingletonManager.props(
                        Bookkeeper.props(new InMemoryBookkeeper()),
                        PoisonPill.getInstance(),
                        ClusterSingletonManagerSettings.create(system)),
                "bookkeeper");

        return system.actorOf(
                ClusterSingletonProxy.props("/user/bookkeeper", ClusterSingletonProxySettings.create(system)),
                "bookkeeperProxy");
    }

    @Provides
    @Singleton
    @Named("status-cache")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideStatusCache(
            final ActorSystem system,
            @Named("bookkeeper-proxy") final ActorRef bookkeeperProxy,
            @Named("periodic-statistics") final ActorRef periodicStats,
            final MetricsFactory metricsFactory) {
        final Cluster cluster = Cluster.get(system);
        final ActorRef clusterStatusCache = system.actorOf(ClusterStatusCache.props(cluster, metricsFactory), "cluster-status");
        return system.actorOf(Status.props(bookkeeperProxy, cluster, clusterStatusCache, periodicStats), "status");
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
    @Named("aggregator-lifecycle")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private ActorRef provideAggregatorLifecycleTracker(
            final ActorSystem system,
            @Named("bookkeeper-proxy") final ActorRef bookkeeperProxy) {
        final ActorRef aggLifecycle = system.actorOf(AggregatorLifecycle.props(), "agg-lifecycle");
        aggLifecycle.tell(new AggregatorLifecycle.Subscribe(bookkeeperProxy), bookkeeperProxy);
        return aggLifecycle;
    }

    @Provides
    @Singleton
    @Named("http-server")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private java.util.concurrent.CompletionStage<akka.http.javadsl.ServerBinding> provideHttpServer(
            final ActorSystem system,
            final MetricsFactory metricsFactory) {

        // Create and bind Http server
        final Materializer materializer = ActorMaterializer.create(system);
        final Routes routes = new Routes(
                system,
                metricsFactory,
                _configuration.getHttpHealthCheckPath(),
                _configuration.getHttpStatusPath());
        final Http http = Http.get(system);
        final akka.stream.javadsl.Source<IncomingConnection, CompletionStage<ServerBinding>> binding = http.bind(
                ConnectHttp.toHost(
                        _configuration.getHttpHost(),
                        _configuration.getHttpPort()),
                materializer);
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
        return clusterSharding.start(
                "Aggregator",
                GuiceActorCreator.props(injector, AggregationRouter.class),
                ClusterShardingSettings.create(system),
                extractor,
                new ParallelLeastShardAllocationStrategy(
                        rebalanceConfiguration.getMaxParallel(),
                        rebalanceConfiguration.getThreshold(),
                        Optional.of(system.actorSelection("/user/cluster-status"))),
                PoisonPill.getInstance());
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
    private AggMessageExtractor provideExtractor() {
        return new AggMessageExtractor();
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
    @Named("circonus-partition-set")
    @SuppressFBWarnings("UPM_UNCALLED_PRIVATE_METHOD") // Invoked reflectively by Guice
    private PartitionSet provideDatabasePartitionSet(@Named("metrics_clusteragg") final Database database) {
        final com.arpnetworking.clusteraggregator.models.ebean.PartitionSet partitionSet =
                com.arpnetworking.clusteraggregator.models.ebean.PartitionSet.findOrCreate(
                        "circonus-partition-set",
                        database,
                        1000,
                        Integer.MAX_VALUE);
        return new DatabasePartitionSet(database, partitionSet);
    }

    private final ClusterAggregatorConfiguration _configuration;

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

        /**
         *{@inheritDoc}
         */
        @Override
        public Database get() {
            return new Database(_name, _configuration);
        }

        private final String _name;
        private final DatabaseConfiguration _configuration;
    }
}
