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

import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import it.unimi.dsi.fastutil.doubles.Double2LongMap;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

/**
 * Tests the HistogramStatistic class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class HistogramStatisticTest {
    @Test
    public void histogramAccumulateQuantities() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(100L, histogram.getEntriesCount());
        for (final Double2LongMap.Entry entry : histogram.getValues()) {
            Assert.assertEquals(1L, entry.getLongValue());
        }
    }

    @Test
    public void histogramAccumulateHistogram() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> merged = HISTOGRAM_STATISTIC.createCalculator();

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }

        merged.accumulate(accumulator.calculate(Collections.emptyMap()));

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = merged.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(100L, histogram.getEntriesCount());
        for (final Double2LongMap.Entry entry : histogram.getValues()) {
            Assert.assertEquals(1L, entry.getLongValue());
        }
    }

    @Test
    public void histogramAccumulateMultipleHistogram() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> merged = HISTOGRAM_STATISTIC.createCalculator();

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator1 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator1.accumulate(new Quantity.Builder().setValue((double) x).build());
        }

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator2 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator2.accumulate(new Quantity.Builder().setValue((double) 10 * x + 1000).build());
        }

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator3 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 50; x <= 100; ++x) {
            accumulator3.accumulate(new Quantity.Builder().setValue((double) x).build());
        }

        merged.accumulate(accumulator1.calculate(Collections.emptyMap()));
        merged.accumulate(accumulator2.calculate(Collections.emptyMap()));
        merged.accumulate(accumulator3.calculate(Collections.emptyMap()));

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = merged.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(251L, histogram.getEntriesCount());
        for (final Double2LongMap.Entry entry : histogram.getValues()) {
            final int val = (int) entry.getDoubleKey();
            if (val < 50) {
                Assert.assertEquals("incorrect value for key " + val, 1L, entry.getLongValue());
            } else if (val <= 100) {
                Assert.assertEquals("incorrect value for key " + val, 2L, entry.getLongValue());
            } else { // val > 100
                Assert.assertEquals("incorrect value for key " + val, 1L, entry.getLongValue());
            }
        }

        Assert.assertEquals(2000d, histogram.getValueAtPercentile(99.9d), 1d);
    }

    /**
     * Check that neither the entries count or bucket values overflow an integer while accumulating.
     */
    @Test
    public void histogramAccumulateHistogramsCheckNoOverflow() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> merged = HISTOGRAM_STATISTIC.createCalculator();

        for (int i = 0; i < 3; ++i) {
            final HistogramStatistic.Histogram histogram = new HistogramStatistic.Histogram();
            histogram.recordValue(0, 1000000000);
            final HistogramStatistic.HistogramSupportingData supportingData = new HistogramStatistic.HistogramSupportingData.Builder()
                    .setHistogramSnapshot(histogram.getSnapshot())
                    .build();
            final CalculatedValue<HistogramStatistic.HistogramSupportingData> calculatedValue =
                    new CalculatedValue.Builder<HistogramStatistic.HistogramSupportingData>()
                        .setValue(new Quantity.Builder().setValue((double) 1000000000).build())
                        .setData(supportingData)
                        .build();
            merged.accumulate(calculatedValue);
        }

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = merged.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(3000000000L, histogram.getEntriesCount());
        for (final Double2LongMap.Entry entry : histogram.getValues()) {
            if (entry.getDoubleKey() == 0) {
                Assert.assertEquals(3000000000L, entry.getLongValue());
            }
        }
    }

    @Test
    public void histogramQuantityConversion() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        accumulator.accumulate(new Quantity.Builder().setValue((double) 1).setUnit(Unit.SECOND).build());
        accumulator.accumulate(new Quantity.Builder().setValue((double) 1000).setUnit(Unit.MILLISECOND).build());

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(2, histogram.getEntriesCount());
        Assert.assertEquals(1, histogram.getValues().size());
    }

    @Test(expected = IllegalStateException.class)
    public void histogramQuantityInvalidConversion() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        accumulator.accumulate(new Quantity.Builder().setValue((double) 1).setUnit(null).build());
        accumulator.accumulate(new Quantity.Builder().setValue((double) 1000).setUnit(Unit.MILLISECOND).build());

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        Assert.assertEquals(2, histogram.getEntriesCount());
        Assert.assertEquals(1, histogram.getValues().size());
    }

    @Test
    public void histogramAccumulateMultipleHistogramConversion() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> merged = HISTOGRAM_STATISTIC.createCalculator();

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator1 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator1.accumulate(new Quantity.Builder().setValue((double) x).setUnit(Unit.SECOND).build());
        }

        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator2 = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator2.accumulate(new Quantity.Builder().setValue((double) 1000 * x).setUnit(Unit.MILLISECOND).build());
        }

        merged.accumulate(accumulator1.calculate(Collections.emptyMap()));
        merged.accumulate(accumulator2.calculate(Collections.emptyMap()));

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = merged.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        for (final Double2LongMap.Entry entry : histogram.getValues()) {

            Assert.assertTrue(entry.getDoubleKey() <= 100.0);
        }

        Assert.assertEquals(200, histogram.getEntriesCount());
    }

    @Test
    public void histogramUnitConversion() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 1; x <= 100; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x * 1000).setUnit(Unit.MILLISECOND).build());
        }

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();
        for (final Double2LongMap.Entry entry : histogram.getValues()) {
            final int val = (int) entry.getDoubleKey();
            if (val < 990) {
                Assert.fail("shouldn't see a key this small");
            }
        }

        final HistogramStatistic.HistogramSupportingData converted = supportingData.toUnit(Unit.SECOND);
        histogram = converted.getHistogramSnapshot();
        for (final Double2LongMap.Entry entry : histogram.getValues()) {
            final int val = (int) entry.getDoubleKey();
            if (val > 100) {
                Assert.fail("shouldn't see a key this large after unit conversion");
            }
        }

        Assert.assertEquals(99.5d, histogram.getValueAtPercentile(99.9d), 1d);
        Assert.assertEquals(100, histogram.getEntriesCount());
    }

    @Test
    public void histogramEnds() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        accumulator.accumulate(new Quantity.Builder().setValue((double) 10).setUnit(Unit.MILLISECOND).build());
        accumulator.accumulate(new Quantity.Builder().setValue((double) 50).setUnit(Unit.MILLISECOND).build());

        final CalculatedValue<HistogramStatistic.HistogramSupportingData> value = accumulator.calculate(Collections.emptyMap());
        final HistogramStatistic.HistogramSupportingData supportingData = value.getData();
        final HistogramStatistic.HistogramSnapshot histogram = supportingData.getHistogramSnapshot();

        Assert.assertEquals(10d, histogram.getValueAtPercentile(0), 1d);
        Assert.assertEquals(50d, histogram.getValueAtPercentile(100), 1d);
        Assert.assertEquals(2, histogram.getEntriesCount());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final HistogramStatistic HISTOGRAM_STATISTIC = (HistogramStatistic) STATISTIC_FACTORY.getStatistic("histogram");
}
