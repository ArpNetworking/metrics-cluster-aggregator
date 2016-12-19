/**
 * Copyright 2016 Groupon.com
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

import com.arpnetworking.clusteraggregator.models.CombinedMetricData;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.wavefront.agent.PointHandler;
import com.wavefront.agent.PointHandlerImpl;
import com.wavefront.agent.PushAgent;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.joda.time.Period;
import org.joda.time.format.ISOPeriodFormat;
import sunnylabs.report.ReportPoint;

import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Publishes aggregations to Wavefront via Wavefront's proxy used as a library. This class is thread safe.
 *
 * @author Gil Markham (gil@groupon.com)
 */
public final class WavefrontSink extends BaseSink {
    /**
     * {@inheritDoc}
     */
    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        for (final AggregatedData aggregatedData : periodicData.getData()) {
            if (!aggregatedData.isSpecified()) {
                continue;
            }
            final ReportPoint.Builder reportPointBuilder = ReportPoint.newBuilder();
            final FQDSN fqdsn = aggregatedData.getFQDSN();
            reportPointBuilder.setMetric(buildMetricName(fqdsn, periodicData.getPeriod()));
            reportPointBuilder.setHost(periodicData.getDimensions().get("host"));
            reportPointBuilder.setValue(aggregatedData.getValue().getValue());
            reportPointBuilder.setTimestamp(periodicData.getStart().getMillis());
            reportPointBuilder.setTable("dummy");

            // Build the list of annotations based on cluster and white listed dimensions
            final Map<String, String> annotations = Maps.newHashMap();
            annotations.put("cluster", fqdsn.getCluster());
            if (_whiteListedDimensions.isPresent()) {
                // Respect the whitelist.
                final ImmutableSet<String> whiteList = _whiteListedDimensions.get();
                for (final Map.Entry<String, String> dimension : periodicData.getDimensions().entrySet()) {
                    if (whiteList.contains(dimension.getKey())) {
                        annotations.put(dimension.getKey(), dimension.getValue());
                    }
                }
            } else {
                // Don't use the whitelist, send everything.
                for (final Map.Entry<String, String> dimension : periodicData.getDimensions().entrySet()) {
                    final String k = dimension.getKey();
                    if (!k.equals(CombinedMetricData.CLUSTER_KEY)
                            && !k.equals(CombinedMetricData.HOST_KEY)
                            && !k.equals(CombinedMetricData.SERVICE_KEY)) {
                        annotations.put(dimension.getKey(), dimension.getValue());
                    }
                }
            }

            reportPointBuilder.setAnnotations(annotations);

            // Report the actual data point
            _pointHandler.reportPoint(reportPointBuilder.build(), "");
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
    }

    private String buildMetricName(final FQDSN fqdsn, final Period period) {
        return fqdsn.getService()
                + "."
                + period.toString(ISOPeriodFormat.standard())
                + "."
                + fqdsn.getMetric().replace("/", ".")
                + "."
                + fqdsn.getStatistic().getName().replace(".", "_");
    }

    private List<String> buildAgentArgs() {
        final List<String> argList = new ArrayList<>();

        // Host parameter
        argList.add("-h");
        argList.add(_uri.toString());

        // Retry threads if set
        if (_retryThreads.isPresent()) {
            argList.add("--retryThreads");
            argList.add(_retryThreads.get().toString());
        }

        // Flush threads if set
        if (_flushThreads.isPresent()) {
            argList.add("--flushThreads");
            argList.add(_flushThreads.get().toString());
        }

        // Purge buffer
        argList.add("--purgeBuffer");
        argList.add(_purgeBuffer ? "true" : "false");

        // Push Flush Interval if set
        if (_pushFlushInterval.isPresent()) {
            argList.add("--pushFlushInterval");
            argList.add(_pushFlushInterval.get().toString());
        }

        // Push Flush Max Points if set
        if (_pushFlushMaxPoints.isPresent()) {
            argList.add("--pushFlushMaxPoints");
            argList.add(_pushFlushMaxPoints.get().toString());
        }

        // Buffer base name if set
        if (_bufferBasename.isPresent()) {
            argList.add("--buffer");
            argList.add(_bufferBasename.get());
        }

        // Id File
        argList.add("--idFile");
        argList.add(_idFile);

        return argList;
    }

    private WavefrontSink(final Builder builder) {
        super(builder);
        _uri = builder._uri;
        _token = builder._token;
        _retryThreads = Optional.ofNullable(builder._retryThreads);
        _flushThreads = Optional.ofNullable(builder._flushThreads);
        _purgeBuffer = builder._purgeBuffer;
        _pushFlushInterval = Optional.ofNullable(builder._pushFlushInterval);
        _pushFlushMaxPoints = Optional.ofNullable(builder._pushFlushMaxPoints);
        _idFile = builder._idFile;
        _whiteListedDimensions = Optional.ofNullable(builder._whiteListedDimensions);
        _bufferBasename = Optional.ofNullable(builder._bufferBasename);
        final WavefrontAgent agent = AgentFactory.getInstance(_token, buildAgentArgs());
        _pointHandler = agent.createPointHandler();
    }

    private final URI _uri;
    private final String _token;
    private final Optional<Integer> _retryThreads;
    private final Optional<Integer> _flushThreads;
    private final boolean _purgeBuffer;
    private final Optional<Long> _pushFlushInterval;
    private final Optional<Integer> _pushFlushMaxPoints;
    private final String _idFile;
    private final PointHandler _pointHandler;
    private final Optional<ImmutableSet<String>> _whiteListedDimensions;
    private final Optional<String> _bufferBasename;

    private static final Logger LOGGER = LoggerFactory.getLogger(WavefrontSink.class);

    /**
     * Implementation of builder pattern for <code>WavefrontSink</code>.
     *
     * @author Gil Markham (gil at groupon dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, WavefrontSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(WavefrontSink::new);
        }

        /**
         * The token to auto-register sink with an account. Cannot be null or empty.
         *
         * @param value Token to register with an account.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setToken(final String value) {
            _token = value;
            return self();
        }

        /**
         * The <code>URI</code> to post the aggregated data to. Cannot be null.
         *
         * @param value The <code>URI</code> to post the aggregated data to.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setUri(final URI value) {
            _uri = value;
            return self();
        }

        /**
         * The number of threads retrying failed transmissions. Defaults to number of processors (min. 4).
         *
         * @param value The number of retry threads.
         * @return This instance of <code>Builder</code>
         */
        public Builder setRetryThreads(final Integer value) {
            _retryThreads = value;
            return self();
        }

        /**
         * The number of threads that flush data to the server. Defaults to number of processors (min. 4).
         *
         * @param value The number of flush threads.
         * @return This instance of <code>Builder</code>
         */
        public Builder setFlushThreads(final Integer value) {
            _flushThreads = value;
            return self();
        }

        /**
         * Whether to purge the retry buffer on start-up. Defaults to false.
         *
         * @param value Whether to purge buffer on start-up.
         * @return This instance of <code>Builder</code>
         */
        public Builder setPurgeBuffer(final Boolean value) {
            _purgeBuffer = value;
            return self();
        }

        /**
         * Milliseconds between flushes.  Defaults to 1000ms.
         *
         * @param value Milliseconds between flushes.
         * @return This instance of <code>Builder</code>
         */
        public Builder setPushFlushInterval(final Long value) {
            _pushFlushInterval = value;
            return self();
        }

        /**
         * Maximum allowed points in a single push flush. Defaults to 50,000.
         *
         * @param value Maximum points allowed per flush.
         * @return This instance of <code>Builder</code>
         */
        public Builder setPushFlushMaxPoints(final Integer value) {
            _pushFlushMaxPoints = value;
            return self();
        }

        /**
         * File to read agent id from. Cannot be null or empty.
         *
         * @param value Id file name.
         * @return This instance of <code>Builder</code>
         */
        public Builder setIdFile(final String value) {
            _idFile = value;
            return self();
        }

        /**
         * Set of dimension names to be passed through as a Wavefront annotation/tag.
         *
         * @param value Set of white listed dimension names
         * @return This instance of <code>Builder</code>
         */
        public Builder setWhiteListedDimensions(final Set<String> value) {
            _whiteListedDimensions = new ImmutableSet.Builder<String>().addAll(value).build();
            return self();
        }

        /**
         * Base name for Wavefront agent buffer files. Defaults to 'buffer' if not specified.
         *
         * @param value Base name of buffer files.
         * @return This instance of <code>Builder</code>
         */
        public Builder setBufferBasename(final String value) {
            _bufferBasename = value;
            return self();
        }


        /**
         * {@inheritDoc}
         */
        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private URI _uri;
        @NotNull
        @NotEmpty
        private String _token;
        private Integer _retryThreads;
        private Integer _flushThreads;
        @NotNull
        private Boolean _purgeBuffer = false;
        private Long _pushFlushInterval;
        private Integer _pushFlushMaxPoints;
        @NotNull
        @NotEmpty
        private String _idFile;
        private ImmutableSet<String> _whiteListedDimensions = null;
        private String _bufferBasename;
    }

    private static final class AgentFactory {
        private static final ConcurrentHashMap<String, WavefrontAgent> AGENT_MAP = new ConcurrentHashMap<>(2);

        public static synchronized WavefrontAgent getInstance(final String token, final List<String> arguments) {
            return AGENT_MAP.computeIfAbsent(token, key -> {
                final WavefrontAgent agent = new WavefrontAgent();
                final List<String> fullArgs = new ArrayList<>();
                fullArgs.add("-t");
                fullArgs.add(key);
                fullArgs.addAll(arguments);
                try {
                    agent.start(fullArgs.toArray(new String[fullArgs.size()]));
                } catch (final IOException e) {
                    LOGGER.warn()
                            .setMessage("Error configuring Wavefront Agent")
                            .setThrowable(e)
                            .log();
                }
                return agent;
            });
        }
    }

    private static final class WavefrontAgent extends PushAgent {
        @Override
        protected void startListeners() {
        }

        PointHandler createPointHandler() {
            return new PointHandlerImpl(-1, pushValidationLevel, pushFlushMaxPoints, getFlushTasks(-1));
        }
    }
}
