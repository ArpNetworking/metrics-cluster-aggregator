/*
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

import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableSet;
import net.sf.oval.constraint.NotNull;

import java.util.Collections;
import java.util.concurrent.CompletionStage;

/**
 * Filtering sink for excluding data based on dimensions present or absent.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class DimensionFilteringSink extends BaseSink {

    @Override
    public void recordAggregateData(final PeriodicData data) {
        if (!data.getDimensions().keySet().containsAll(_excludeWithoutDimensions)) {
            // Excluded data missing required dimension.
            return;
        }
        if (!Collections.disjoint(data.getDimensions().keySet(), _excludeWithDimensions)) {
            // Excluded data with specified dimension(s).
            return;
        }
        _sink.recordAggregateData(data);
    }

    @Override
    public void close() {
        _sink.close();
    }

    @Override
    public CompletionStage<Void> shutdownGracefully() {
        return _sink.shutdownGracefully();
    }

    private DimensionFilteringSink(final Builder builder) {
        super(builder);
        _excludeWithoutDimensions = builder._excludeWithoutDimensions;
        _excludeWithDimensions = builder._excludeWithDimensions;
        _sink = builder._sink;
    }

    private final ImmutableSet<String> _excludeWithoutDimensions;
    private final ImmutableSet<String> _excludeWithDimensions;
    private final Sink _sink;

    /**
     * Implementation of builder pattern for {@link DimensionFilteringSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends BaseSink.Builder<Builder, DimensionFilteringSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(DimensionFilteringSink::new);
        }

        /**
         * Sets exclude without dimensions. Exclude any periodic data without all of
         * these dimensions present.
         *
         * @param value The exclude without dimensions.
         * @return This instance of {@link Builder}.
         */
        public Builder setExcludeWithoutDimensions(final ImmutableSet<String> value) {
            _excludeWithoutDimensions = value;
            return self();
        }

        /**
         * Sets exclude with dimensions. Exclude any periodic data with any one of
         * these dimensions present.
         *
         * @param value The exclude with dimensions.
         * @return This instance of {@link Builder}.
         */
        public Builder setExcludeWithDimensions(final ImmutableSet<String> value) {
            _excludeWithDimensions = value;
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
        private ImmutableSet<String> _excludeWithoutDimensions = ImmutableSet.of();
        @NotNull
        private ImmutableSet<String> _excludeWithDimensions = ImmutableSet.of();
        @NotNull
        private Sink _sink;
    }
}
