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
package com.arpnetworking.clusteraggregator.configuration;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.google.common.base.MoreObjects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import net.sf.oval.constraint.Range;
import net.sf.oval.constraint.ValidateWithMethod;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Optional;

/**
 * Representation of cluster aggregator configuration.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class ClusterAggregatorConfiguration {
    /**
     * Create an {@link com.fasterxml.jackson.databind.ObjectMapper} for cluster aggregator configuration.
     *
     * @return An {@link ObjectMapper} for TsdAggregator configuration.
     */
    public static ObjectMapper createObjectMapper() {
        return ObjectMapperFactory.getInstance();
    }

    public String getMonitoringCluster() {
        return _monitoringCluster;
    }

    public String getMonitoringService() {
        return _monitoringService;
    }

    public ImmutableList<JsonNode> getMonitoringSinks() {
        return _monitoringSinks;
    }

    @Deprecated
    public Optional<String> getMonitoringHost() {
        return _monitoringHost;
    }

    @Deprecated
    public Optional<Integer> getMonitoringPort() {
        return _monitoringPort;
    }

    public int getHttpPort() {
        return _httpPort;
    }

    public String getHttpHost() {
        return _httpHost;
    }

    public String getHttpHealthCheckPath() {
        return _httpHealthCheckPath;
    }

    public String getHttpStatusPath() {
        return _httpStatusPath;
    }

    public String getHttpVersionPath() {
        return _httpVersionPath;
    }

    public File getLogDirectory() {
        return _logDirectory;
    }

    public Duration getMaxConnectionTimeout() {
        return _maxConnectionTimeout;
    }

    public Duration getMinConnectionTimeout() {
        return _minConnectionTimeout;
    }

    public Duration getJvmMetricsCollectionInterval() {
        return _jvmMetricsCollectionInterval;
    }

    public Map<String, ?> getAkkaConfiguration() {
        return Collections.unmodifiableMap(_akkaConfiguration);
    }

    public File getHostPipelineConfiguration() {
        return _hostPipelineConfiguration;
    }

    public File getClusterPipelineConfiguration() {
        return _clusterPipelineConfiguration;
    }

    public ImmutableSet<String> getReaggregationDimensions() {
        return _reaggregationDimensions;
    }

    public boolean getReaggregationInjectClusterAsHost() {
        return _reaggregationInjectClusterAsHost;
    }

    public Duration getReaggregationTimeout() {
        return _reaggregationTimeout;
    }

    public Optional<Duration> getAggregatorLivelinessTimeout() {
        return _aggregatorLivelinessTimeout;
    }

    public RebalanceConfiguration getRebalanceConfiguration() {
        return _rebalanceConfiguration;
    }

    public Map<String, DatabaseConfiguration> getDatabaseConfigurations() {
        return _databaseConfigurations;
    }

    public int getAggregationPort() {
        return _aggregationPort;
    }

    public String getAggregationHost() {
        return _aggregationHost;
    }

    public String getClusterHostSuffix() {
        return _clusterHostSuffix;
    }

    public boolean getCalculateClusterAggregations() {
        return _calculateClusterAggregations;
    }

    @Override
    public String toString() {
        return MoreObjects.toStringHelper(this)
                .add("id", Integer.toHexString(System.identityHashCode(this)))
                .add("MonitoringCluster", _monitoringCluster)
                .add("MonitoringService", _monitoringService)
                .add("MonitoringSinks", _monitoringSinks)
                .add("MonitoringHost", _monitoringHost)
                .add("MonitoringPort", _monitoringPort)
                .add("JvmMetricsCollectionInterval", _jvmMetricsCollectionInterval)
                .add("HttpHost", _httpHost)
                .add("HttpPort", _httpPort)
                .add("HttpHealthCheckPath", _httpHealthCheckPath)
                .add("HttpStatusPath", _httpStatusPath)
                .add("HttpVersionPath", _httpVersionPath)
                .add("AggregatorHost", _aggregationHost)
                .add("AggregatorPort", _aggregationPort)
                .add("LogDirectory", _logDirectory)
                .add("AkkaConfiguration", _akkaConfiguration)
                .add("HostPipelineConfiguration", _hostPipelineConfiguration)
                .add("ClusterPipelineConfiguration", _hostPipelineConfiguration)
                .add("ReaggregationDimensions", _reaggregationDimensions)
                .add("ReaggregationInjectClusterAsHost", _reaggregationInjectClusterAsHost)
                .add("ReaggregationTimeout", _reaggregationTimeout)
                .add("AggregatorLivelinessTimeout", _aggregatorLivelinessTimeout)
                .add("MinConnectionTimeout", _minConnectionTimeout)
                .add("MaxConnectionTimeout", _maxConnectionTimeout)
                .add("RebalanceConfiguration", _rebalanceConfiguration)
                .add("ClusterHostSuffix", _clusterHostSuffix)
                .add("DatabaseConfigurations", _databaseConfigurations)
                .toString();
    }

    private ClusterAggregatorConfiguration(final Builder builder) {
        _monitoringCluster = builder._monitoringCluster;
        _monitoringService = builder._monitoringService;
        _monitoringSinks = builder._monitoringSinks;
        _monitoringHost = Optional.ofNullable(builder._monitoringHost);
        _monitoringPort = Optional.ofNullable(builder._monitoringPort);
        _httpHost = builder._httpHost;
        _httpPort = builder._httpPort;
        _httpHealthCheckPath = builder._httpHealthCheckPath;
        _httpStatusPath = builder._httpStatusPath;
        _httpVersionPath = builder._httpVersionPath;
        _aggregationHost = builder._aggregationHost;
        _aggregationPort = builder._aggregationPort;
        _logDirectory = builder._logDirectory;
        _akkaConfiguration = Maps.newHashMap(builder._akkaConfiguration);
        _hostPipelineConfiguration = builder._hostPipelineConfiguration;
        _clusterPipelineConfiguration = builder._clusterPipelineConfiguration;
        _reaggregationDimensions = builder._reaggregationDimensions;
        _reaggregationInjectClusterAsHost = builder._reaggregationInjectClusterAsHost;
        _reaggregationTimeout = builder._reaggregationTimeout;
        _aggregatorLivelinessTimeout = builder._aggregatorLivelinessTimeout;
        _minConnectionTimeout = builder._minConnectionTimeout;
        _maxConnectionTimeout = builder._maxConnectionTimeout;
        _jvmMetricsCollectionInterval = builder._jvmMetricsCollectionInterval;
        _rebalanceConfiguration = builder._rebalanceConfiguration;
        _clusterHostSuffix = builder._clusterHostSuffix;
        _calculateClusterAggregations = builder._calculateClusterAggregations;
        _databaseConfigurations = Maps.newHashMap(builder._databaseConfigurations);
    }

    private final String _monitoringCluster;
    private final String _monitoringService;
    private final ImmutableList<JsonNode> _monitoringSinks;
    private final Optional<String> _monitoringHost;
    private final Optional<Integer> _monitoringPort;
    private final File _logDirectory;
    private final String _httpHost;
    private final int _httpPort;
    private final String _httpHealthCheckPath;
    private final String _httpStatusPath;
    private final String _httpVersionPath;
    private final String _aggregationHost;
    private final int _aggregationPort;
    private final Map<String, ?> _akkaConfiguration;
    private final File _clusterPipelineConfiguration;
    private final File _hostPipelineConfiguration;
    private final ImmutableSet<String> _reaggregationDimensions;
    private final boolean _reaggregationInjectClusterAsHost;
    private final Duration _reaggregationTimeout;
    private final Optional<Duration> _aggregatorLivelinessTimeout;
    private final Duration _minConnectionTimeout;
    private final Duration _maxConnectionTimeout;
    private final Duration _jvmMetricsCollectionInterval;
    private final RebalanceConfiguration _rebalanceConfiguration;
    private final String _clusterHostSuffix;
    private final boolean _calculateClusterAggregations;
    private final Map<String, DatabaseConfiguration> _databaseConfigurations;

    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementation for
     * {@link com.arpnetworking.clusteraggregator.configuration.ClusterAggregatorConfiguration}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class Builder extends OvalBuilder<ClusterAggregatorConfiguration> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(ClusterAggregatorConfiguration::new);

            final ObjectNode sinkRoot = OBJECT_MAPPER.createObjectNode();
            sinkRoot.set("class", new TextNode("com.arpnetworking.metrics.impl.ApacheHttpSink"));
            _monitoringSinks = ImmutableList.of(sinkRoot);
        }

        /**
         * The monitoring cluster. Cannot be null or empty.
         *
         * @param value The monitoring cluster.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringCluster(final String value) {
            _monitoringCluster = value;
            return this;
        }

        /**
         * The monitoring service. Optional. Cannot be null or empty. Default
         * is cluster_aggregator.
         *
         * @param value The monitoring service.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringService(final String value) {
            _monitoringService = value;
            return this;
        }

        /**
         * The monitoring sinks. Optional. Cannot be null. The default value is
         * the default instance of {@link com.arpnetworking.metrics.impl.ApacheHttpSink}.
         *
         * @param value The monitoring sinks.
         * @return This instance of {@link Builder}.
         */
        public Builder setMonitoringSinks(final ImmutableList<JsonNode> value) {
            _monitoringSinks = value;
            return this;
        }

        /**
         * The monitoring endpoint host (Where to post data). Optional. Cannot
         * be empty. Defaults to unspecified.
         *
         * @param value The monitoring endpoint uri.
         * @return This instance of {@link Builder}.
         * @deprecated Use {@link Builder#setMonitoringSinks(ImmutableList)}
         */
        @Deprecated
        public Builder setMonitoringHost(final String value) {
            _monitoringHost = value;
            return this;
        }

        /**
         * The monitoring endpoint port. Optional. Must be between 1 and
         * 65535 (inclusive). Defaults to unspecified.
         *
         * @param value The port to listen on.
         * @return This instance of {@link Builder}.
         * @deprecated Use {@link Builder#setMonitoringSinks(ImmutableList)}
         */
        @Deprecated
        public Builder setMonitoringPort(final Integer value) {
            _monitoringPort = value;
            return this;
        }

        /**
         * The http host address to bind to. Cannot be null or empty.
         *
         * @param value The host address to bind to.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpHost(final String value) {
            _httpHost = value;
            return this;
        }

        /**
         * The suffix to append to the cluster host when reporting metrics. Optional.
         * Cannot be null.  Default is the empty string.
         *
         * @param value The host suffix to append.
         * @return This instance of {@link Builder}.
         */
        public Builder setClusterHostSuffix(final String value) {
            _clusterHostSuffix = value;
            return this;
        }

        /**
         * The aggregation server host address to bind to. Cannot be null or empty.
         *
         * @param value The host address to bind to.
         * @return This instance of {@link Builder}.
         */
        public Builder setAggregationHost(final String value) {
            _aggregationHost = value;
            return this;
        }

        /**
         * The http port to listen on. Cannot be null, must be between 1 and
         * 65535 (inclusive).
         *
         * @param value The port to listen on.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpPort(final Integer value) {
            _httpPort = value;
            return this;
        }

        /**
         * The http health check path. Cannot be null or empty. Optional. Default is "/ping".
         *
         * @param value The health check path.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpHealthCheckPath(final String value) {
            _httpHealthCheckPath = value;
            return this;
        }

        /**
         * The http status path. Cannot be null or empty. Optional. Default is "/status".
         *
         * @param value The status path.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpStatusPath(final String value) {
            _httpStatusPath = value;
            return this;
        }

        /**
         * The http version path. Cannot be null or empty. Optional. Default is "/version".
         *
         * @param value The version path.
         * @return This instance of {@link Builder}.
         */
        public Builder setHttpVersionPath(final String value) {
            _httpVersionPath = value;
            return this;
        }

        /**
         * The http port to listen on. Cannot be null, must be between 1 and
         * 65535 (inclusive). Defaults to 7065.
         *
         * @param value The port to listen on.
         * @return This instance of {@link Builder}.
         */
        public Builder setAggregationPort(final Integer value) {
            _aggregationPort = value;
            return this;
        }

        /**
         * Akka configuration. Cannot be null. By convention Akka configuration
         * begins with a map containing a single key "akka" and a value of a
         * nested map. For more information please see:
         *
         * http://doc.akka.io/docs/akka/snapshot/general/configuration.html
         *
         * NOTE: No validation is performed on the Akka configuration itself.
         *
         * @param value The Akka configuration.
         * @return This instance of {@link Builder}.
         */
        public Builder setAkkaConfiguration(final Map<String, ?> value) {
            _akkaConfiguration = value;
            return this;
        }

        /**
         * The log directory. Cannot be null.
         *
         * @param value The log directory.
         * @return This instance of {@link Builder}.
         */
        public Builder setLogDirectory(final File value) {
            _logDirectory = value;
            return this;
        }

        /**
         * The minimum connection cycling time for a client.  Required.  Cannot be null.
         *
         * @param value The minimum time before cycling a connection.
         * @return This instance of {@link Builder}.
         */
        public Builder setMinConnectionTimeout(final Duration value) {
            _minConnectionTimeout = value;
            return this;
        }

        /**
         * The maximum connection cycling time for a client.  Required.  Cannot be null.
         *
         * @param value The maximum time before cycling a connection.
         * @return This instance of {@link Builder}.
         */
        public Builder setMaxConnectionTimeout(final Duration value) {
            _maxConnectionTimeout = value;
            return this;
        }

        /**
         * Period for collecting JVM metrics.
         *
         * @param value A {@link Duration} value.
         * @return This instance of {@link Builder}.
         */
        public Builder setJvmMetricsCollectionInterval(final Duration value) {
            _jvmMetricsCollectionInterval = value;
            return this;
        }

        /**
         * The cluster pipeline configuration file. Cannot be null.
         *
         * @param value The cluster pipeline configuration file.
         * @return This instance of {@link Builder}.
         */
        public Builder setClusterPipelineConfiguration(final File value) {
            _clusterPipelineConfiguration = value;
            return this;
        }

        /**
         * The host pipeline configuration file. Cannot be null.
         *
         * @param value The host pipeline configuration file.
         * @return This instance of {@link Builder}.
         */
        public Builder setHostPipelineConfiguration(final File value) {
            _hostPipelineConfiguration = value;
            return this;
        }

        /**
         * The reaggregation dimensions. Optional. Default is set containing
         * {@code host}. Cannot be null.
         *
         * @param value The regaggregation dimensions.
         * @return This instance of {@link Builder}.
         */
        public Builder setReaggregationDimensions(final ImmutableSet<String> value) {
            _reaggregationDimensions = value;
            return this;
        }

        /**
         * Whether to inject a {@code host} dimension with a value based on
         * the {@code cluster} dimension. Optional. Default is {@code True}.
         * Cannot be null.
         *
         * @param value Whether to inject {@code host} derived from {@code cluster}.
         * @return This instance of {@link Builder}.
         */
        public Builder setReaggregationInjectClusterAsHost(final Boolean value) {
            _reaggregationInjectClusterAsHost = value;
            return this;
        }

        /**
         * The time from period start to wait for all data to arrive. This
         * should include any timeout in MAD plus any send spread/jitter in
         * MAD plus the actual desired time to wait in CAGG. Optional. Default
         * is {@code PT1M}. Cannot be null.
         *
         * @param value Timeout from period start to wait for all data to arrive.
         * @return This instance of {@link Builder}.
         */
        public Builder setReaggregationTimeout(final Duration value) {
            _reaggregationTimeout = value;
            return this;
        }

        /**
         * How often an aggregator actor should check for liveliness. An actor is considered live
         * if any data is received between consecutive checks.
         *
         * This control is useful for culling instances which aggregate very infrequent dimension
         * sets, especially if the application itself is long-lived.
         *
         * This must be greater than the reaggregation timeout, otherwise an actor could be
         * incorrectly marked as stale before flushing its data.
         *
         * Optional. Defaults to twice the reaggregation timeout. Cannot be null.
         *
         * @param value Timeout between consecutive liveliness checks.
         * @return This instance of {@link Builder}.
         */
        public Builder setAggregatorLivelinessTimeout(final Duration value) {
            _aggregatorLivelinessTimeout = Optional.of(value);
            return this;
        }

        /**
         * Configuration for the shard rebalance settings.
         *
         * @param value The rebalacing configuration.
         * @return This instance of {@link Builder}.
         */
        public Builder setRebalanceConfiguration(final RebalanceConfiguration value) {
            _rebalanceConfiguration = value;
            return this;
        }

        /**
         * Configuration for the databases.
         *
         * @param value The database configurations.
         * @return This instance of {@link Builder}.
         */
        public Builder setDatabaseConfigurations(final Map<String, DatabaseConfiguration> value) {
            _databaseConfigurations = value;
            return this;
        }

        /**
         * Whether or not to perform cluster-level aggregations. When using a datasource that supports
         * native histograms, turning this off will reduce cpu cost. Optional. Defaults to true.
         *
         * @param value true to perform cluster aggregations, false to just forward host data.
         * @return This instance of {@link Builder}.
         */
        public Builder setCalculateClusterAggregations(final Boolean value) {
            _calculateClusterAggregations = value;
            return this;
        }

        /**
         * Validate that the aggregator liveliness timeout is greater than the reaggregation timeout.
         *
         * @param aggregatorLivelinessTimeout the configured liveliness timeout
         * @return true if the given value is valid
         */
        @SuppressFBWarnings(value = "UPM_UNCALLED_PRIVATE_METHOD", justification = "invoked reflectively by @ValidateWithMethod")
        public boolean validateAggregatorLivelinessTimeout(final Optional<Duration> aggregatorLivelinessTimeout) {
            return aggregatorLivelinessTimeout
                    .map(duration -> duration.compareTo(_reaggregationTimeout) > 0)
                    .orElse(true);
        }

        @NotNull
        @NotEmpty
        private String _monitoringCluster;
        @NotNull
        @NotEmpty
        private String _monitoringService = "cluster_aggregator";
        // TODO(ville): Apply the default here once we migrate off JsonNode.
        @NotNull
        private ImmutableList<JsonNode> _monitoringSinks;
        @NotEmpty
        private String _monitoringHost;
        @Range(min = 1, max = 65535)
        private Integer _monitoringPort;
        @NotNull
        private Duration _jvmMetricsCollectionInterval = Duration.ofMillis(1000);
        @NotNull
        @NotEmpty
        private String _httpHost = "0.0.0.0";
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _httpPort = 7066;
        @NotNull
        @NotEmpty
        private String _httpHealthCheckPath = "/ping";
        @NotNull
        @NotEmpty
        private String _httpStatusPath = "/status";
        @NotNull
        @NotEmpty
        private String _httpVersionPath = "/version";
        @NotNull
        @NotEmpty
        private String _aggregationHost = "0.0.0.0";
        @NotNull
        @Range(min = 1, max = 65535)
        private Integer _aggregationPort = 7065;
        @NotNull
        private File _logDirectory;
        @NotNull
        private File _clusterPipelineConfiguration;
        @NotNull
        private ImmutableSet<String> _reaggregationDimensions = ImmutableSet.of();
        @NotNull
        private Boolean _reaggregationInjectClusterAsHost = Boolean.TRUE;
        @NotNull
        private Duration _reaggregationTimeout = Duration.ofMinutes(1);
        @NotNull
        @ValidateWithMethod(methodName = "validateAggregatorLivelinessTimeout", parameterType = Optional.class)
        private Optional<Duration> _aggregatorLivelinessTimeout = Optional.empty();
        @NotNull
        private File _hostPipelineConfiguration;
        @NotNull
        private Map<String, ?> _akkaConfiguration;
        @NotNull
        private Duration _maxConnectionTimeout;
        @NotNull
        private Duration _minConnectionTimeout;
        @NotNull
        private RebalanceConfiguration _rebalanceConfiguration;
        @NotNull
        private String _clusterHostSuffix = "";
        @NotNull
        private Boolean _calculateClusterAggregations = true;
        @NotNull
        private Map<String, DatabaseConfiguration> _databaseConfigurations = Maps.newHashMap();
    }
}
