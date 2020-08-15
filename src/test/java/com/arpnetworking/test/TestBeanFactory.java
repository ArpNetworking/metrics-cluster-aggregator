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
package com.arpnetworking.test;

import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.base.Function;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.time.Duration;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;
import javax.annotation.Nullable;

/**
 * Creates reasonable random instances of common data types for testing. This is
 * strongly preferred over mocking data type classes as mocking should be
 * reserved for defining behavior and not data.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class TestBeanFactory {

    /**
     * Create a builder for pseudo-random {@link Condition}.
     *
     * @return New builder for pseudo-random {@link Condition}.
     */
    public static Condition.Builder createConditionBuilder() {
        return new Condition.Builder()
                .setFQDSN(createFQDSN())
                .setName("condition-" + UUID.randomUUID())
                .setExtensions(ImmutableMap.of("severity", "severity-" + UUID.randomUUID()))
                .setThreshold(createSample())
                .setTriggered(Boolean.TRUE);
    }

    /**
     * Create a new reasonable pseudo-random {@link Condition}.
     *
     * @return New reasonable pseudo-random {@link Condition}.
     */
    public static Condition createCondition() {
        return createConditionBuilder().build();
    }

    /**
     * Create a builder for pseudo-random {@link FQDSN}.
     *
     * @return New builder for pseudo-random {@link FQDSN}.
     */
    public static FQDSN.Builder createFQDSNBuilder() {
        return new FQDSN.Builder()
                .setStatistic(MEAN_STATISTIC)
                .setService("service-" + UUID.randomUUID())
                .setMetric("metric-" + UUID.randomUUID())
                .setCluster("cluster-" + UUID.randomUUID());
    }

    /**
     * Create a new reasonable pseudo-random {@link FQDSN}.
     *
     * @return New reasonable pseudo-random {@link FQDSN}.
     */
    public static FQDSN createFQDSN() {
        return createFQDSNBuilder().build();
    }

    /**
     * Create a builder for pseudo-random {@link AggregatedData}.
     *
     * @return New builder for pseudo-random {@link AggregatedData}.
     */
    public static AggregatedData.Builder createAggregatedDataBuilder() {
        return new AggregatedData.Builder()
                .setFQDSN(createFQDSN())
                .setHost("host-" + UUID.randomUUID())
                .setValue(createSample())
                .setStart(ZonedDateTime.now())
                .setPeriod(Duration.ofMinutes(5))
                .setIsSpecified(true)
                .setSamples(Lists.newArrayList(createSample()))
                .setPopulationSize((long) (Math.random() * 100));
    }

    /**
     * Create a new reasonable pseudo-random {@link AggregatedData}.
     *
     * @return New reasonable pseudo-random {@link AggregatedData}.
     */
    public static AggregatedData createAggregatedData() {
        return createAggregatedDataBuilder().build();
    }

    /**
     * Create a builder for pseudo-random {@link PeriodicData}.
     *
     * @return New builder for pseudo-random {@link PeriodicData}.
     */
    public static PeriodicData.Builder createPeriodicDataBuilder() {
        return new PeriodicData.Builder()
                .setDimensions(ImmutableMap.of("host", "host-" + UUID.randomUUID()))
                .setData(ImmutableList.of(createAggregatedData()))
                .setConditions(ImmutableList.of())
                .setPeriod(Duration.ofMinutes(5))
                .setStart(ZonedDateTime.now());
    }

    /**
     * Create a new reasonable pseudo-random {@link PeriodicData}.
     *
     * @return New reasonable pseudo-random {@link PeriodicData}.
     */
    public static PeriodicData createPeriodicData() {
        return createPeriodicDataBuilder().build();
    }

    /**
     * Create a builder for reasonable pseudo-random {@link Quantity}.
     *
     * @return New builder for reasonable pseudo-random {@link Quantity}.
     */
    public static Quantity.Builder createSampleBuilder() {
        return new Quantity.Builder().setValue(Math.random()).setUnit(Unit.BIT);
    }

    /**
     * Create a new reasonable pseudo-random {@link Quantity}.
     *
     * @return New reasonable pseudo-random {@link Quantity}.
     */
    public static Quantity createSample() {
        return new Quantity.Builder().setValue(Math.random()).setUnit(Unit.BIT).build();
    }

    /**
     * Create a {@link List} of {@link Quantity} instances in
     * {@link Unit#MILLISECOND} from a {@link List} of {@link Double}
     * values.
     *
     * @param values The values.
     * @return {@link List} of {@link Quantity} instances.
     */
    public static List<Quantity> createSamples(final List<Double> values) {
        return FluentIterable.from(Lists.newArrayList(values)).transform(CREATE_SAMPLE).toList();
    }

    private static final Function<Double, Quantity> CREATE_SAMPLE = new Function<Double, Quantity>() {
        @Override
        public Quantity apply(@Nullable final Double input) {
            if (input == null) {
                throw new IllegalArgumentException("Sample value cannot be null");
            }
            return new Quantity.Builder().setValue(input).setUnit(Unit.MILLISECOND).build();
        }
    };

    private TestBeanFactory() {}

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");
}
