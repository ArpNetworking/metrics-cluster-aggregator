/*
 * Copyright 2018 Dropbox
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
package com.arpnetworking.tsdcore.model;

import com.arpnetworking.clusteraggregator.models.AggregationMode;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.logback.annotations.Loggable;
import com.google.common.collect.ImmutableList;
import net.sf.oval.constraint.NotNull;

/**
 * Encapsulates a {@code List} of {@link AggregationMessage} instances and
 * an {@link AggregationMode} to be applied to those messages.
 *
 * @author Ville Koskela (vkoskela at dropbox dot com)
 */
@Loggable
public final class AggregationRequest {

    public ImmutableList<AggregationMessage> getAggregationMessages() {
        return _aggregationMessages;
    }

    public AggregationMode getAggregationMode() {
        return _aggregationMode;
    }

    private AggregationRequest(final Builder builder) {
        _aggregationMessages = builder._aggregationMessages;
        _aggregationMode = builder._aggregationMode;
    }

    private final ImmutableList<AggregationMessage> _aggregationMessages;
    private final AggregationMode _aggregationMode;

    /**
     * {@code Builder} implementation for {@link AggregationRequest}.
     */
    public static final class Builder extends OvalBuilder<AggregationRequest> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(AggregationRequest::new);
        }

        /**
         * Set the aggregation messages. Required. Cannot be null.
         *
         * @param aggregationMessages The aggregation messages.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setAggregationMessages(final ImmutableList<AggregationMessage> aggregationMessages) {
            _aggregationMessages = aggregationMessages;
            return this;
        }

        /**
         * Set the aggregation mode. Required. Cannot be null.
         *
         * @param aggregationMode The aggregation mode.
         * @return This instance of <code>Builder</code>.
         */
        public Builder setAggregationMode(final AggregationMode aggregationMode) {
            _aggregationMode = aggregationMode;
            return this;
        }

        @NotNull
        private ImmutableList<AggregationMessage> _aggregationMessages;
        @NotNull
        private AggregationMode _aggregationMode;
    }
}
