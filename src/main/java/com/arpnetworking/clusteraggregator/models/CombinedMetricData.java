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
package com.arpnetworking.clusteraggregator.models;

import akka.util.ByteString;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregationMessage;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.arpnetworking.tsdcore.statistics.HistogramStatistic;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;
import net.sf.oval.constraint.NotNull;
import org.joda.time.DateTime;
import org.joda.time.Period;

import java.time.Duration;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * A metric-based aggregation model.  Holds all of the statistics and supporting for a metric on a host.
 * These will be combined in an {@link com.arpnetworking.clusteraggregator.aggregation.AggregationRouter} to
 * produce the set of cluster statistics.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class CombinedMetricData {
    /**
     * Private constructor.
     */
    private CombinedMetricData(final Builder builder) {
        _metricName = builder._metricName;
        _period = builder._period;
        _calculatedValues = builder._calculatedValues;
        _periodStart = builder._periodStart;
        _minRequestTime = Optional.ofNullable(builder._minRequestTime);
        _service = builder._service;
        _cluster = builder._cluster;
    }

    public String getMetricName() {
        return _metricName;
    }

    public Period getPeriod() {
        return _period;
    }

    public Map<Statistic, StatisticValue> getCalculatedValues() {
        return _calculatedValues;
    }

    public DateTime getPeriodStart() {
        return _periodStart;
    }

    public Optional<DateTime> getMinRequestTime() {
        return _minRequestTime;
    }

    public String getService() {
        return _service;
    }

    public String getCluster() {
        return _cluster;
    }

    /**
     * Compute the population size for a metric from its statistic values.
     *
     * @param metricName the name of the metric
     * @param statisticValues the statistic values for the metric.
     * @return requires {@link com.arpnetworking.tsdcore.statistics.CountStatistic}
     * or {@link HistogramStatistic} to return an accurate count otherwise
     * returns 1
     */
    public static long computePopulationSize(
            final String metricName,
            final Map<Statistic, CombinedMetricData.StatisticValue> statisticValues) {
        final ImmutableMap.Builder<Statistic, CalculatedValue<?>> requiredCalculatedValues = ImmutableMap.builder();
        @Nullable final CombinedMetricData.StatisticValue countStatisticValue = statisticValues.get(COUNT_STATISTIC);
        if (countStatisticValue != null) {
            requiredCalculatedValues.put(COUNT_STATISTIC, countStatisticValue.getValue());
        }
        @Nullable final CombinedMetricData.StatisticValue histogramStatisticValue = statisticValues.get(HISTOGRAM_STATISTIC);
        if (countStatisticValue != null) {
            requiredCalculatedValues.put(HISTOGRAM_STATISTIC, histogramStatisticValue.getValue());
        }
        return computePopulationSizeFromCalculatedValues(
                metricName,
                requiredCalculatedValues.build());
    }

    /**
     * Compute the population size for a metric from its calculated values.
     *
     * @param metricName the name of the metric
     * @param calculatedValues the calculated values for the metric.
     * @return requires {@link com.arpnetworking.tsdcore.statistics.CountStatistic}
     * or {@link HistogramStatistic} to return an accurate count otherwise
     * returns 1
     */
    public static long computePopulationSizeFromCalculatedValues(
            final String metricName,
            final Map<Statistic, CalculatedValue<?>> calculatedValues) {
        // Compute the population size either via the count statistic or
        // via histogram bin counting.
        @Nullable final CalculatedValue<?> histogramCalculatedValue = calculatedValues.get(HISTOGRAM_STATISTIC);
        @Nullable final CalculatedValue<?> countCalculatedValue = calculatedValues.get(COUNT_STATISTIC);

        // Prefer using the histogram since it's value is always accurate
        if (histogramCalculatedValue != null) {
            if (histogramCalculatedValue.getData() instanceof HistogramStatistic.HistogramSupportingData) {
                final HistogramStatistic.HistogramSupportingData supportingData =
                        (HistogramStatistic.HistogramSupportingData) histogramCalculatedValue.getData();
                return supportingData.getHistogramSnapshot().getEntriesCount();
            }
        }

        // Fallback to using the count since it's value is a double
        if (countCalculatedValue != null) {
            return (long) countCalculatedValue.getValue().getValue();
        }

        // Take some backwards compatible default behavior, but let's log that
        // it's a problem.
        NO_POPULATION_SIZE_LOGGER.warn()
                .setMessage("Unable to compute population size")
                .addData("metric", metricName)
                .log();
        return 1L;
    }

    /**
     * The key used in dimensions to specify the host-dimension.
     */
    public static final String HOST_KEY = "host";
    /**
     * The key used in dimensions to specify the service-dimension.
     */
    public static final String SERVICE_KEY = "service";
    /**
     * The key used in dimensions to specify the cluster-dimension.
     */
    public static final String CLUSTER_KEY = "cluster";

    private final String _metricName;
    private final Period _period;
    private final Map<Statistic, StatisticValue> _calculatedValues;
    private final DateTime _periodStart;
    private final Optional<DateTime> _minRequestTime;
    private final String _service;
    private final String _cluster;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic COUNT_STATISTIC = STATISTIC_FACTORY.getStatistic("count");
    private static final Statistic HISTOGRAM_STATISTIC = STATISTIC_FACTORY.getStatistic("histogram");
    private static final Logger NO_POPULATION_SIZE_LOGGER =
            LoggerFactory.getRateLimitLogger(CombinedMetricData.class, Duration.ofSeconds(30));

    /**
     * {@link com.arpnetworking.commons.builder.Builder} implementationfor
     * {@link CombinedMetricData}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static class Builder extends OvalBuilder<CombinedMetricData> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(CombinedMetricData::new);
        }

        /**
         * Set the name of the service. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder setService(final String value) {
            _service = value;
            return this;
        }

        /**
         * Set the name of the metric. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder setMetricName(final String value) {
            _metricName = value;
            return this;
        }

        /**
         * Set the name of the cluster. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder setCluster(final String value) {
            _cluster = value;
            return this;
        }

        /**
         * Set the start of the period. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder setPeriodStart(final DateTime value) {
            _periodStart = value;
            return this;
        }

        /**
         * Set the period. Required. Cannot be null.
         *
         * @param value The value.
         * @return This {@link Builder} instance.
         */
        public Builder setPeriod(final Period value) {
            _period = value;
            return this;
        }

        /**
         * Set the minimum request time. May be null.
         *
         * @param minRequestTime The value to set
         * @return This {@link Builder} instance.
         */
        public Builder setMinRequestTime(@Nullable final DateTime minRequestTime) {
            _minRequestTime = minRequestTime;
            return this;
        }

        private static Optional<DateTime> maybeParseMinRequestTime(final String minRequestTime) {
            if (minRequestTime.isEmpty()) {
                return Optional.empty();
            }

            return Optional.of(DateTime.parse(minRequestTime));
        }

        /**
         * Initializes the builder with fields from a {@link com.arpnetworking.metrics.aggregation.protocol.Messages.StatisticSetRecord}.
         *
         * @param record The record.
         * @return This {@link com.arpnetworking.clusteraggregator.models.CombinedMetricData.Builder}.
         */
        public static Builder fromStatisticSetRecord(final Messages.StatisticSetRecord record) {
            final Builder builder = new Builder()
                    .setMetricName(record.getMetric())
                    .setPeriod(Period.parse(record.getPeriod()))
                    .setPeriodStart(DateTime.parse(record.getPeriodStart()))
                    .setMinRequestTime(maybeParseMinRequestTime(record.getClientMinimumRequestTime()).orElse(null))
                    .setCluster(record.getCluster())
                    .setService(record.getService());
            for (final Messages.StatisticRecord statisticRecord : record.getStatisticsList()) {

                final Optional<Statistic> statisticOptional = STATISTIC_FACTORY.tryGetStatistic(statisticRecord.getStatistic());
                if (!statisticOptional.isPresent()) {
                    LOGGER.warn()
                            .setMessage("Cannot build CombinedMetricData from StatisticSetRecord, unknown statistic")
                            .addData("statistic", statisticRecord.getStatistic())
                            .log();
                    continue;
                }
                final Statistic statistic = statisticOptional.get();
                final CalculatedValue.Builder<?> calculatedValueBuilder;
                final Quantity quantity = getQuantity(statisticRecord);
                //Turn the data in the message into ComputedData's
                if (statistic instanceof HistogramStatistic) {
                    final Messages.SparseHistogramSupportingData supportingData = deserialzeSupportingData(statisticRecord);

                    final HistogramStatistic.Histogram histogram = new HistogramStatistic.Histogram();
                    for (final Messages.SparseHistogramEntry entry : supportingData.getEntriesList()) {
                        final double bucket = entry.getBucket();
                        final long count = entry.getCount();
                        histogram.recordValue(bucket, count);
                    }

                    final HistogramStatistic.HistogramSupportingData histogramSupportingData =
                            new HistogramStatistic.HistogramSupportingData.Builder()
                                    .setHistogramSnapshot(histogram.getSnapshot())
                                    .setUnit(getUnitFromName(supportingData.getUnit()))
                                    .build();

                    calculatedValueBuilder = new CalculatedValue.Builder<HistogramStatistic.HistogramSupportingData>()
                            .setData(histogramSupportingData);

                } else {
                    calculatedValueBuilder = new CalculatedValue.Builder<Void>();
                }

                calculatedValueBuilder.setValue(quantity);
                builder._calculatedValues.put(
                        statistic,
                        new StatisticValue(calculatedValueBuilder.build(), statisticRecord.getUserSpecified()));
            }
            return builder;
        }

        private static Quantity getQuantity(final Messages.StatisticRecord statisticRecord) {
            final Optional<Unit> unit;
            unit = getUnitFromName(statisticRecord.getUnit());
            return new Quantity.Builder()
                    .setUnit(unit.orElse(null))
                    .setValue(statisticRecord.getValue())
                    .build();
        }

        private static Optional<Unit> getUnitFromName(final String unitString) {
            final Optional<Unit> unit;
            if (Strings.isNullOrEmpty(unitString)) {
                unit = Optional.empty();
            } else {
                unit = Optional.ofNullable(Unit.valueOf(unitString));
            }
            return unit;
        }

        @SuppressWarnings("unchecked")
        private static <T> T deserialzeSupportingData(final Messages.StatisticRecord record) {
            if (record.getSupportingData() == null) {
                throw new RuntimeException(new IllegalArgumentException("no supporting data found"));
            }
            return (T) AggregationMessage.deserialize(
                    ByteString.fromByteBuffer(record.getSupportingData().asReadOnlyByteBuffer())).get().getMessage();
        }

        @NotNull
        private String _metricName;
        @NotNull
        private Period _period;
        @NotNull
        private DateTime _periodStart;
        @Nullable
        private DateTime _minRequestTime;
        @NotNull
        private String _service;
        @NotNull
        private String _cluster;
        @NotNull
        private Map<Statistic, StatisticValue> _calculatedValues = Maps.newHashMap();

        private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
        private static final Logger LOGGER = LoggerFactory.getLogger(Builder.class);
    }

    /**
     * Representation of a computed statistic and related data.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static class StatisticValue {
        /**
         * Public constructor.
         *
         * @param value The calculated value.
         * @param userSpecified Whether the statistic is desired as output.
         */
        public StatisticValue(final CalculatedValue<?> value, final boolean userSpecified) {
            _value = value;
            _userSpecified = userSpecified;
        }

        public Boolean getUserSpecified() {
            return _userSpecified;
        }

        public CalculatedValue<?> getValue() {
            return _value;
        }

        private final CalculatedValue<?> _value;
        private final Boolean _userSpecified;
    }
}
