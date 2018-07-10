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
package com.arpnetworking.clusteraggregator.models;

/**
 * The aggregation mode applied to data.
 *
 * @author Ville Koskela (vkoskela at dropbox dot com)
 */
public enum AggregationMode {

    /**
     * Persist the statistics data as provided by the client.
     */
    PERSIST(true, false),

    /**
     * Reaggregate the statistics data provided by the client and persist only
     * the reaggregated data.
     */
    REAGGREGATE(false, true),

    /**
     * Persist the statistics data as provided by the client <b>AND</b> reaggregate
     * the statistics data provided by the client persisting the reaggregated data
     * as well.
     */
    PERSIST_AND_REAGGREGATE(true, true);

    /**
     * Whether this {@link AggregationMode} should persist client statistics.
     *
     * @return True if and only if client statistics should be persisted
     */
    public boolean shouldPersist() {
        return _shouldPersist;
    }

    /**
     * Whether this {@link AggregationMode} should reaggregate client statistics.
     *
     * @return True if and only if client statistics should be reaggregated
     */
    public boolean shouldReaggregate() {
        return _shouldReaggregate;
    }

    AggregationMode(final boolean shouldPersist, final boolean shouldReaggregate) {
        _shouldPersist = shouldPersist;
        _shouldReaggregate = shouldReaggregate;
    }

    private final boolean _shouldPersist;
    private final boolean _shouldReaggregate;
}
