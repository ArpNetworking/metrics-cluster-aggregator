/**
 * Copyright 2016 Groupon.com
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
import com.google.common.collect.ImmutableMap;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Matchers;
import org.mockito.Mockito;

import java.util.Collections;

/**
 * Test for HostNameFilteringSink.
 *
 * @author Matthew Hayter (mhayter at groupon dot com)
 */
public class HostNameFilteringSinkTest {

    @Before
    public void setUp() {
        _mockSink = Mockito.mock(Sink.class);
        _sinkBuilder = new HostNameFilteringSink.Builder()
                .setName("filtering_sink_test")
                .setSink(_mockSink);
    }

    @Test
    public void testIncludeByDefault() {
        final Sink sink = _sinkBuilder.build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicData();
        sink.recordAggregateData(periodicData);
        Mockito.verify(_mockSink).recordAggregateData(Matchers.eq(periodicData));
    }

    @Test
    public void testExcludeOverDefault() {
        final Sink sink = _sinkBuilder
                .setExcludeFilters(Collections.singletonList(".*HOSTNAMEMATCH.*"))
                .build();
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setMetric("Some Metric")
                                .build())
                        .build());
        final PeriodicData excludedPeriodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .setDimensions(ImmutableMap.of(
                        "host", "myHostWithHOSTNAMEMATCHhere"
                ))
                .build();
        sink.recordAggregateData(excludedPeriodicData);
        Mockito.verify(_mockSink, Mockito.never()).recordAggregateData(Matchers.any(PeriodicData.class));
    }

    @Test
    public void testIncludeOverExclude() {
        final Sink sink = _sinkBuilder
                .setExcludeFilters(Collections.singletonList(".*MATCHES HERE.*"))
                .setIncludeFilters(Collections.singletonList(".*for inclusion.*"))
                .build();
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setMetric("Some Metric")
                                .build())
                        .build());
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .setDimensions(ImmutableMap.of(
                        "host", "Host name MATCHES HERE for inclusion"
                ))
                .build();
        sink.recordAggregateData(periodicData);
        Mockito.verify(_mockSink).recordAggregateData(Matchers.eq(periodicData));
    }

    private HostNameFilteringSink.Builder _sinkBuilder;
    private Sink _mockSink;
}
