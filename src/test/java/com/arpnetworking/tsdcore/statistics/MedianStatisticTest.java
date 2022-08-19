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

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.Lists;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Tests the MedianStatistic class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class MedianStatisticTest {

    @Test
    public void testName() {
        Assert.assertEquals("median", MEDIAN_STATISTIC.getName());
    }

    @Test
    public void testAliases() {
        final Statistic statistic = MEDIAN_STATISTIC;
        Assert.assertEquals(2, statistic.getAliases().size());
        Assert.assertTrue(statistic.getAliases().contains("tp50"));
        Assert.assertTrue(statistic.getAliases().contains("p50"));
    }

    @Test
    public void testMedianStat() {
        final Statistic tp = MEDIAN_STATISTIC;
        final ArrayList<Double> vList = Lists.newArrayList();
        for (int x = 0; x < 100; ++x) {
            vList.add((double) x);
        }
        final List<Quantity> vals = TestBeanFactory.createSamples(vList);
        final Quantity calculated = tp.calculate(vals);
        MatcherAssert.assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setValue(50.0)
                                .setUnit(Unit.MILLISECOND)
                                .build()));
    }

    @Test
    public void testEquality() {
        Assert.assertFalse(MEDIAN_STATISTIC.equals(null));
        Assert.assertFalse(MEDIAN_STATISTIC.equals("ABC"));
        Assert.assertTrue(MEDIAN_STATISTIC.equals(MEDIAN_STATISTIC));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(MEDIAN_STATISTIC.hashCode(), MEDIAN_STATISTIC.hashCode());
    }

    @Test
    public void testCalculator() {
        final Accumulator<HistogramStatistic.HistogramSupportingData> accumulator = HISTOGRAM_STATISTIC.createCalculator();
        for (int x = 0; x < 100; ++x) {
            accumulator.accumulate(new Quantity.Builder().setValue((double) x).build());
        }
        final CalculatedValue<Void> calculated = MEDIAN_STATISTIC.createCalculator().calculate(
                Collections.singletonMap(HISTOGRAM_STATISTIC, accumulator));
        Assert.assertTrue(areClose(new Quantity.Builder().setValue(50.0).build(), calculated.getValue()));
    }


    private boolean areClose(final Quantity expected, final Quantity actual) {
        final double diff = Math.abs(expected.getValue() - actual.getValue());
        return diff / expected.getValue() <= (0.01 * expected.getValue());
    }

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final MedianStatistic MEDIAN_STATISTIC = (MedianStatistic) STATISTIC_FACTORY.getStatistic("median");
    private static final HistogramStatistic HISTOGRAM_STATISTIC = (HistogramStatistic) STATISTIC_FACTORY.getStatistic("histogram");
}
