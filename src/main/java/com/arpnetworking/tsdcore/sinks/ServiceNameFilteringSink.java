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

import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import net.sf.oval.constraint.NotNull;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletionStage;
import java.util.regex.Pattern;

/**
 * A publisher that wraps another, filters the metrics with regular expressions,
 * and forwards included metrics to the wrapped sink. This  class is thread
 * safe.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class ServiceNameFilteringSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData periodicData) {
        final ImmutableList.Builder<AggregatedData> filteredDataBuilder = ImmutableList.builder();
        for (final AggregatedData datum : periodicData.getData()) {
            final String service = datum.getFQDSN().getService();
            final Boolean cachedResult = _cachedFilterResult.getUnchecked(service);
            if (cachedResult) {
                filteredDataBuilder.add(datum);
            }
        }

        final ImmutableList.Builder<Condition> filteredConditionsBuilder = ImmutableList.builder();
        for (final Condition condition : periodicData.getConditions()) {
            final String service = condition.getFQDSN().getService();
            final Boolean cachedResult = _cachedFilterResult.getUnchecked(service);
            if (cachedResult) {
                filteredConditionsBuilder.add(condition);
            }
        }

        final ImmutableList<AggregatedData> filteredData = filteredDataBuilder.build();
        final ImmutableList<Condition> filteredConditions = filteredConditionsBuilder.build();
        if (!filteredData.isEmpty() || !filteredConditions.isEmpty()) {
            _sink.recordAggregateData(
                    PeriodicData.Builder.clone(periodicData, new PeriodicData.Builder())
                            .setData(filteredData)
                            .setConditions(filteredConditions)
                            .build());
        }
    }

    @Override
    public void close() {
        _sink.close();
    }

    @Override
    public CompletionStage<Void> shutdownGracefully() {
        return _sink.shutdownGracefully();
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    @Override
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("super", super.toLogValue())
                .put("excludeFilters", _excludeFilters)
                .put("includeFilters", _includeFilters)
                .put("sink", _sink)
                .build();
    }

    private boolean includeMetric(final String metric) {
        for (final Pattern includeFilter : _includeFilters) {
            if (includeFilter.matcher(metric).matches()) {
                return true;
            }
        }
        for (final Pattern excludeFilter : _excludeFilters) {
            if (excludeFilter.matcher(metric).matches()) {
                return false;
            }
        }
        return true;
    }

    private static List<Pattern> compileExpressions(final List<String> expressions) {
        final List<Pattern> patterns = Lists.newArrayListWithExpectedSize(expressions.size());
        for (final String expression : expressions) {
            patterns.add(Pattern.compile(expression));
        }
        return patterns;
    }

    /**
     * Protected constructor.
     *
     * @param builder Instance of {@link Builder}.
     */
    protected ServiceNameFilteringSink(final Builder builder) {
        super(builder);
        _cachedFilterResult = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .build(new CacheLoader<String, Boolean>() {
                    @Override
                    public Boolean load(final String key) throws Exception {
                        return includeMetric(key);
                    }
                });
        _excludeFilters = compileExpressions(builder._excludeFilters);
        _includeFilters = compileExpressions(builder._includeFilters);
        _sink = builder._sink;
    }

    private final LoadingCache<String, Boolean> _cachedFilterResult;
    private final List<Pattern> _excludeFilters;
    private final List<Pattern> _includeFilters;
    private final Sink _sink;

    /**
     * Base {@link Builder} implementation.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, ServiceNameFilteringSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(ServiceNameFilteringSink::new);
        }

        /**
         * Sets exclude filters. Exclude filters are regular expressions matched
         * against metric names. Include filters take precedence over exclude
         * filters and the default is to include if neither applies. Cannot be
         * null.
         *
         * @param value The exclude filters.
         * @return This instance of {@link Builder}.
         */
        public Builder setExcludeFilters(final List<String> value) {
            _excludeFilters = value;
            return self();
        }

        /**
         * Sets include filters. Include filters are regular expressions matched
         * against metric names. Include filters take precedence over exclude
         * filters and the default is to include if neither applies. Cannot be
         * null.
         *
         * @param value The include filters.
         * @return This instance of {@link Builder}.
         */
        public Builder setIncludeFilters(final List<String> value) {
            _includeFilters = value;
            return self();
        }

        /**
         * The sink to wrap. Cannot be null.
         *
         * @param value The sink to wrap.
         * @return This instance of {@link Builder}.
         */
        public Builder setSink(final Sink value) {
            _sink = value;
            return this;
        }

        @Override
        protected Builder self() {
            return this;
        }

        @NotNull
        private List<String> _excludeFilters = Collections.emptyList();
        @NotNull
        private List<String> _includeFilters = Collections.emptyList();
        @NotNull
        private Sink _sink;
    }
}
