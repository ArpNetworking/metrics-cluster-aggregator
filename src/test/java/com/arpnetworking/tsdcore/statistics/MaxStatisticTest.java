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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.CalculatedValue;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.model.Unit;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.Test;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Tests the TPStatistic class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class MaxStatisticTest {

    @Test
    public void testName() {
        assertEquals("max", MAX_STATISTIC.getName());
    }

    @Test
    public void testStat() {
        final Statistic tp100 = MAX_STATISTIC;
        final List<Double> doubleVals = Lists.newArrayList(ONE_TO_FIVE);
        final List<Quantity> vals = TestBeanFactory.createSamples(doubleVals);
        final Quantity calculated = tp100.calculate(vals);
        assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setValue(5.0)
                                .setUnit(Unit.MILLISECOND)
                                .build()));
    }

    @Test
    public void testEquality() {
        assertFalse(MAX_STATISTIC.equals(null));
        assertFalse(MAX_STATISTIC.equals("ABC"));
        assertTrue(MAX_STATISTIC.equals(MAX_STATISTIC));
    }

    @Test
    public void testHashCode() {
        assertEquals(MAX_STATISTIC.hashCode(), MAX_STATISTIC.hashCode());
    }

    @Test
    public void testAccumulator() {
        final Accumulator<NullSupportingData> accumulator = (Accumulator<NullSupportingData>) MAX_STATISTIC.createCalculator();
        accumulator.accumulate(new Quantity.Builder().setValue(12d).build());
        accumulator.accumulate(new Quantity.Builder().setValue(18d).build());
        accumulator.accumulate(new Quantity.Builder().setValue(5d).build());
        final CalculatedValue<NullSupportingData> calculated = accumulator.calculate(Collections.emptyMap());
        assertEquals(calculated.getValue(), new Quantity.Builder().setValue(18.0).build());
    }

    private static final List<Double> ONE_TO_FIVE = Lists.newArrayList(1d, 2d, 3d, 4d, 5d);
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final MaxStatistic MAX_STATISTIC = (MaxStatistic) STATISTIC_FACTORY.getStatistic("max");
}
