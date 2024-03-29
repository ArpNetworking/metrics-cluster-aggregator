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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import it.unimi.dsi.fastutil.doubles.Double2LongAVLTreeMap;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import it.unimi.dsi.fastutil.doubles.Double2LongSortedMap;
import it.unimi.dsi.fastutil.objects.ObjectSortedSet;
import net.sf.oval.constraint.NotNull;

import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import javax.annotation.Nullable;

/**
 * Histogram statistic. This is a supporting statistic and does not produce
 * a value itself. It is used by percentile statistics as a common dependency.
 * Use {@link StatisticFactory} for construction.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class HistogramStatistic extends BaseStatistic {

    @Override
    public String getName() {
        return "histogram";
    }

    @Override
    public Accumulator<HistogramSupportingData> createCalculator() {
        return new HistogramAccumulator(this);
    }

    @Override
    public Quantity calculate(final List<Quantity> values) {
        throw new UnsupportedOperationException("Unsupported operation: calculate(List<Quantity>)");
    }

    @Override
    public Quantity calculateAggregations(final List<AggregatedData> aggregations) {
        throw new UnsupportedOperationException("Unsupported operation: calculateAggregations(List<AggregatedData>)");
    }

    private HistogramStatistic() { }

    private static final int DEFAULT_PRECISION = 7;
    private static final long serialVersionUID = 7060886488604176233L;

    /**
     * Accumulator computing the histogram of values. There is a dependency on the
     * histogram accumulator from each percentile statistic's calculator.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    /* package private */ static final class HistogramAccumulator
            extends BaseCalculator<HistogramSupportingData>
            implements Accumulator<HistogramSupportingData> {

        /**
         * Public constructor.
         *
         * @param statistic The {@link Statistic}.
         */
        /* package private */ HistogramAccumulator(final Statistic statistic) {
            super(statistic);
        }

        @Override
        public Accumulator<HistogramSupportingData> accumulate(final Quantity quantity) {
            // TODO(barp): Convert to canonical unit. [NEXT]
            final Optional<Unit> quantityUnit = quantity.getUnit();
            checkUnit(quantityUnit);
            if (_unit.isPresent() && !_unit.equals(quantityUnit)) {
                _histogram.recordValue(
                        new Quantity.Builder()
                                .setUnit(_unit.get())
                                .setValue(_unit.get().convert(quantity.getValue(), quantityUnit.get()))
                                .build().getValue());
            } else {
                _histogram.recordValue(quantity.getValue());
            }

            _unit = Optional.ofNullable(_unit.orElse(quantityUnit.orElse(null)));
            return this;
        }

        @Override
        public Accumulator<HistogramSupportingData> accumulate(final CalculatedValue<HistogramSupportingData> calculatedValue) {
            final Optional<Unit> unit = calculatedValue.getData().getUnit();
            checkUnit(unit);
            if (_unit.isPresent() && !_unit.equals(unit)) {
                _histogram.add(calculatedValue.getData().toUnit(_unit.get()).getHistogramSnapshot());
            } else {
                _histogram.add(calculatedValue.getData().getHistogramSnapshot());
            }

            _unit = Optional.ofNullable(_unit.orElse(unit.orElse(null)));
            return this;
        }

        private void checkUnit(final Optional<Unit> unit) {
            if (_unit.isPresent() != unit.isPresent() && _histogram._entriesCount > 0) {
                throw new IllegalStateException(String.format(
                        "Units must both be present or absent; histogramUnit=%s, otherUnit=%s",
                        _unit,
                        unit));
            }
        }

        @Override
        public CalculatedValue<HistogramSupportingData> calculate(final Map<Statistic, Calculator<?>> dependencies) {
            return new CalculatedValue.Builder<HistogramSupportingData>()
                    .setValue(new Quantity.Builder()
                            .setValue(1.0)
                            .build())
                    .setData(new HistogramSupportingData.Builder()
                            .setHistogramSnapshot(_histogram.getSnapshot())
                            .setUnit(_unit)
                            .build())
                    .build();
        }

        /**
         * Calculate the value at the specified percentile.
         *
         * @param percentile The desired percentile to calculate.
         * @return The value at the desired percentile.
         */
        public Quantity calculate(final double percentile) {
            final HistogramSnapshot snapshot = _histogram.getSnapshot();
            return new Quantity.Builder()
                    .setValue(snapshot.getValueAtPercentile(percentile))
                    .setUnit(_unit.orElse(null))
                    .build();
        }

        private Optional<Unit> _unit = Optional.empty();
        private final Histogram _histogram = new Histogram();
    }

    /**
     * Supporting data based on a histogram.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class HistogramSupportingData implements Serializable {
        /**
         * Public constructor.
         *
         * @param builder The builder.
         */
        public HistogramSupportingData(final Builder builder) {
            _unit = builder._unit;
            _histogramSnapshot = builder._histogramSnapshot;
        }

        public HistogramSnapshot getHistogramSnapshot() {
            return _histogramSnapshot;
        }

        /**
         * Transforms the histogram to a new unit. If there is no unit set,
         * the result is a no-op.
         *
         * @param newUnit the new unit
         * @return a new HistogramSupportingData with the units converted
         */
        public HistogramSupportingData toUnit(final Unit newUnit) {
            if (_unit != null) {
                final Histogram newHistogram = new Histogram();
                for (final Double2LongMap.Entry entry : _histogramSnapshot.getValues()) {
                    final double newBucket = newUnit.convert(entry.getDoubleKey(), _unit);
                    newHistogram.recordValue(newBucket, entry.getLongValue());
                }
                return new HistogramSupportingData.Builder()
                        .setHistogramSnapshot(newHistogram.getSnapshot())
                        .setUnit(Optional.of(newUnit))
                        .build();
            }
            return this;
        }

        public Optional<Unit> getUnit() {
            return Optional.ofNullable(_unit);
        }

        @Nullable
        private final Unit _unit;
        private final HistogramSnapshot _histogramSnapshot;
        private static final long serialVersionUID = 1L;

        /**
         * {@link com.arpnetworking.commons.builder.Builder} implementation for
         * {@link HistogramSupportingData}.
         *
         * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
         */
        public static class Builder extends OvalBuilder<HistogramSupportingData> {
            /**
             * Public constructor.
             */
            public Builder() {
                super(HistogramSupportingData::new);
            }

            /**
             * Sets the histogram. Required. Cannot be null.
             *
             * @param value the histogram
             * @return This {@link Builder} instance.
             */
            public Builder setHistogramSnapshot(final HistogramSnapshot value) {
                _histogramSnapshot = value;
                return this;
            }

            /**
             * Sets the unit. Optional. Cannot be null.
             *
             * @param value the unit
             * @return This {@link Builder} instance.
             */
            public Builder setUnit(final Optional<Unit> value) {
                _unit = value.orElse(null);
                return this;
            }

            @Nullable
            private Unit _unit = null;
            @NotNull
            private HistogramSnapshot _histogramSnapshot;
        }
    }

    /**
     * A simple histogram implementation.
     */
    public static final class Histogram {

        /**
         * Records a value into the histogram.
         *
         * @param value The value of the entry.
         * @param count The number of entries at this value.
         */
        public void recordValue(final double value, final long count) {
            _data.merge(truncateToDouble(value), count, Long::sum);
            _entriesCount += count;
        }

        /**
         * Records a value into the histogram.
         *
         * @param value The value of the entry.
         */
        public void recordValue(final double value) {
            recordValue(value, 1);
        }

        /**
         * Records a packed value into the histogram. The packed values must be at the
         * precision that this {@link Histogram} was declared with!
         *
         * @param packed The packed bucket key.
         * @param count The number of entries at this value.
         */
        public void recordPacked(final long packed, final long count) {
            recordValue(unpack(packed), count);
        }

        /**
         * Adds a histogram snapshot to this one.
         *
         * @param histogramSnapshot The histogram snapshot to add to this one.
         */
        public void add(final HistogramSnapshot histogramSnapshot) {
            for (final Double2LongMap.Entry entry : histogramSnapshot._data.double2LongEntrySet()) {
                _data.merge(entry.getDoubleKey(), entry.getLongValue(), Long::sum);
            }
            _entriesCount += histogramSnapshot._entriesCount;
        }

        public HistogramSnapshot getSnapshot() {
            return new HistogramSnapshot(_data, _entriesCount, _precision);
        }

        long truncateToLong(final double val) {
            return Double.doubleToRawLongBits(val) & _truncateMask;
        }

        double truncateToDouble(final double val) {
            return Double.longBitsToDouble(truncateToLong(val));
        }

        long pack(final double val) {
            final long truncated = truncateToLong(val);
            final long shifted = truncated >> (MANTISSA_BITS - _precision);
            return shifted & _packMask;
        }

        double unpack(final long packed) {
            return Double.longBitsToDouble(packed << (MANTISSA_BITS - _precision));
        }

        /**
         * Public constructor.
         */
        public Histogram() {
            this(DEFAULT_PRECISION);
        }

        /**
         * Public constructor.
         *
         * @param precision the bits of mantissa precision in the bucket key
         */
        public Histogram(final int precision) {
            // TODO(ville): Support variable precision histograms end-to-end.
            if (precision != DEFAULT_PRECISION) {
                throw new IllegalArgumentException("The stack does not fully support variable precision histograms.");
            }

            _precision = precision;
            _truncateMask = BASE_MASK >> _precision;
            _packMask = (1 << (_precision + EXPONENT_BITS + 1)) - 1;
        }

        private long _entriesCount = 0;
        private final Double2LongSortedMap _data = new Double2LongAVLTreeMap();
        private final int _precision;
        private final long _truncateMask;
        private final int _packMask;

        private static final int MANTISSA_BITS = 52;
        private static final int EXPONENT_BITS = 11;
        private static final long BASE_MASK = (1L << (MANTISSA_BITS + EXPONENT_BITS)) >> EXPONENT_BITS;
    }

    /**
     * Represents a snapshot of immutable histogram data.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static final class HistogramSnapshot implements Serializable {
        private HistogramSnapshot(final Double2LongSortedMap data, final long entriesCount, final int precision) {
            _precision = precision;
            _entriesCount = entriesCount;
            _data.putAll(data);
        }

        /**
         * Gets the value of the bucket that corresponds to the percentile.
         *
         * @param percentile the percentile
         * @return The value of the bucket at the percentile.
         */
        public Double getValueAtPercentile(final double percentile) {
            // Always "round up" on fractional samples to bias toward 100%
            // The Math.min is for the case where the computation may be just
            // slightly larger than the _entriesCount and prevents an index out of range.
            final int target = (int) Math.min(Math.ceil(_entriesCount * percentile / 100.0D), _entriesCount);
            long accumulated = 0;
            for (final Double2LongMap.Entry next : _data.double2LongEntrySet()) {
                accumulated += next.getLongValue();
                if (accumulated >= target) {
                    return next.getDoubleKey();
                }
            }
            return 0D;
        }

        public int getPrecision() {
            return _precision;
        }

        public long getEntriesCount() {
            return _entriesCount;
        }

        public ObjectSortedSet<Double2LongMap.Entry> getValues() {
            return _data.double2LongEntrySet();
        }

        private long _entriesCount = 0;
        private final Double2LongAVLTreeMap _data = new Double2LongAVLTreeMap();
        private final int _precision;
        private static final long serialVersionUID = 1L;
    }
}
