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
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import org.hamcrest.Matchers;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.util.Collections;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;


/**
 * Tests for the MeanStatistic class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class MeanStatisticTest {

    @Before
    public void setUp() {
        _openMocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void after() throws Exception {
        _openMocks.close();
    }

    @Test
    public void testGetName() {
        final Statistic stat = MEAN_STATISTIC;
        assertThat(stat.getName(), Matchers.equalTo("mean"));
    }

    @Test
    public void testCalculate() {
        final Statistic stat = MEAN_STATISTIC;
        final List<Double> doubleVals = Lists.newArrayList(12d, 20d, 7d);
        final List<Quantity> vals = TestBeanFactory.createSamples(doubleVals);
        final Quantity calculated = stat.calculate(vals);
        assertThat(
                calculated,
                Matchers.equalTo(
                        new Quantity.Builder()
                                .setValue(13.0)
                                .setUnit(Unit.MILLISECOND)
                                .build()));
    }

    @Test
    public void testCalculateWithNoEntries() {
        final Statistic stat = MEAN_STATISTIC;
        final List<Quantity> vals = Collections.emptyList();
        final Quantity calculated = stat.calculate(vals);
        assertThat(calculated, Matchers.equalTo(new Quantity.Builder().setValue(0.0).build()));
    }

    @Test
    public void testEquality() {
        Assert.assertFalse(MEAN_STATISTIC.equals(null));
        Assert.assertFalse(MEAN_STATISTIC.equals("ABC"));
        Assert.assertTrue(MEAN_STATISTIC.equals(MEAN_STATISTIC));
    }

    @Test
    public void testHashCode() {
        Assert.assertEquals(MEAN_STATISTIC.hashCode(), MEAN_STATISTIC.hashCode());
    }

    @Test
    public void testCalculator() {
        Mockito.doReturn(
                new CalculatedValue.Builder<NullSupportingData>()
                    .setValue(new Quantity.Builder().setValue(45.0).build())
                    .build())
                .when(_sumCalculator).calculate(Mockito.any());
        Mockito.doReturn(
                new CalculatedValue.Builder<NullSupportingData>()
                        .setValue(new Quantity.Builder().setValue(3.0).build())
                        .build())
                .when(_countCalculator).calculate(Mockito.any());

        final Calculator<NullSupportingData> calculator = MEAN_STATISTIC.createCalculator();
        final CalculatedValue<NullSupportingData> calculated = calculator.calculate(ImmutableMap.of(
                COUNT_STATISTIC, _countCalculator,
                SUM_STATISTIC, _sumCalculator));
        Assert.assertEquals(calculated.getValue(), new Quantity.Builder().setValue(15.0).build());
    }

    @Mock
    private Calculator<NullSupportingData> _sumCalculator;
    @Mock
    private Calculator<NullSupportingData> _countCalculator;

    private AutoCloseable _openMocks;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final MeanStatistic MEAN_STATISTIC = (MeanStatistic) STATISTIC_FACTORY.getStatistic("mean");
    private static final CountStatistic COUNT_STATISTIC = (CountStatistic) STATISTIC_FACTORY.getStatistic("count");
    private static final SumStatistic SUM_STATISTIC = (SumStatistic) STATISTIC_FACTORY.getStatistic("sum");
}
