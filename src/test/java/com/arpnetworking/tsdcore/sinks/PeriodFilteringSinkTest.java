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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableList;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.time.Duration;
import java.util.Collections;

/**
 * Tests for the {@link PeriodFilteringSink} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class PeriodFilteringSinkTest {

    @Before
    public void setUp() {
        _openMocks = MockitoAnnotations.openMocks(this);
    }

    @After
    public void after() throws Exception {
        _openMocks.close();
    }

    @Test
    public void testDefaultInclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testDefaultInclude")
                .setSink(_sink)
                .build();
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(Duration.ofMinutes(1))
                        .build());
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1))
                .setData(data)
                .build();

        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void testExclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExclude")
                .setSink(_sink)
                .setExclude(Collections.singleton(Duration.ofMinutes(5)))
                .build();
        final AggregatedData excludedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(Duration.ofMinutes(5))
                        .build();
        final PeriodicData periodicDataIn = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5))
                .setData(ImmutableList.of(excludedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataIn);
        Mockito.verifyNoInteractions(_sink);
    }

    @Test
    public void testExcludeLessThanExclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeLessThanExclude")
                .setSink(_sink)
                .setExcludeLessThan(Duration.ofMinutes(5))
                .build();
        final AggregatedData excludedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(Duration.ofMinutes(1))
                        .build();
        final PeriodicData.Builder periodicDataBuilder = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(1));
        final PeriodicData periodicDataExcluded = periodicDataBuilder
                .setData(ImmutableList.of(excludedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataExcluded);
        Mockito.verifyNoInteractions(_sink);
    }

    @Test
    public void testExcludeLessThanInclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeLessThanInclude")
                .setSink(_sink)
                .setExcludeLessThan(Duration.ofMinutes(5))
                .build();
        final AggregatedData includedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(Duration.ofMinutes(5))
                        .build();
        final PeriodicData.Builder periodicDataBuilder = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5));
        final PeriodicData periodicDataIncluded = periodicDataBuilder
                .setData(ImmutableList.of(includedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataIncluded);
        Mockito.verify(_sink).recordAggregateData(periodicDataIncluded);
    }

    @Test
    public void testExcludeGreaterThanExclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeGreaterThanExclude")
                .setSink(_sink)
                .setExcludeGreaterThan(Duration.ofMinutes(5))
                .build();
        final AggregatedData excludedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(Duration.ofMinutes(10))
                        .build();
        final PeriodicData.Builder periodicDataBuilder = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(10));
        final PeriodicData periodicDataExcluded = periodicDataBuilder
                .setData(ImmutableList.of(excludedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataExcluded);
        Mockito.verifyNoInteractions(_sink);
    }

    @Test
    public void testExcludeGreaterThanInclude() {
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testExcludeGreaterThanInclude")
                .setSink(_sink)
                .setExcludeGreaterThan(Duration.ofMinutes(5))
                .build();
        final AggregatedData includedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(Duration.ofMinutes(5))
                        .build();
        final PeriodicData.Builder periodicDataBuilder = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5));
        final PeriodicData periodicDataIncluded = periodicDataBuilder
                .setData(ImmutableList.of(includedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicDataIncluded);
        Mockito.verify(_sink).recordAggregateData(periodicDataIncluded);
    }

    @Test
    public void testIncludeOverExclude() {
        final Duration includePeriod = Duration.ofMinutes(5);
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testIncludeOverExclude")
                .setSink(_sink)
                .setInclude(Collections.singleton(includePeriod))
                .setExclude(Collections.singleton(includePeriod))
                .build();
        final AggregatedData includedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(includePeriod)
                        .build();
        final PeriodicData.Builder periodicDataBuilder = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(5));
        final PeriodicData periodicData = periodicDataBuilder
                .setData(ImmutableList.of(includedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void testIncludeOverLessThanExclude() {
        final Duration includePeriod = Duration.ofMinutes(5);
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testIncludeOverLessThanExclude")
                .setSink(_sink)
                .setInclude(Collections.singleton(includePeriod))
                .setExcludeLessThan(Duration.ofMinutes(10))
                .build();
        final AggregatedData includedDatum =
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(includePeriod)
                        .build();
        final PeriodicData.Builder periodicDataBuilder = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(10));
        final PeriodicData periodicData = periodicDataBuilder
                .setData(ImmutableList.of(includedDatum))
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Test
    public void testIncludeOverGreaterThanExclude() {
        final Duration includePeriod = Duration.ofMinutes(5);
        final PeriodFilteringSink periodFilteringSink = new PeriodFilteringSink.Builder()
                .setName("testIncludeOverGreaterThanExclude")
                .setSink(_sink)
                .setInclude(Collections.singleton(includePeriod))
                .setExcludeGreaterThan(Duration.ofMinutes(10))
                .build();
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setPeriod(includePeriod)
                        .build());
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Duration.ofMinutes(10))
                .setData(data)
                .build();
        periodFilteringSink.recordAggregateData(periodicData);
        Mockito.verify(_sink).recordAggregateData(periodicData);
    }

    @Mock
    private Sink _sink;

    private AutoCloseable _openMocks;
}
