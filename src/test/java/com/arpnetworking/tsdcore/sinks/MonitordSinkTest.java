/*
 * Copyright 2014 Brandon Arp
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

import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.collect.ImmutableList;
import org.apache.pekko.actor.ActorSystem;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.net.URI;
import java.time.Duration;
import java.util.Collection;

/**
 * Tests for the {@link MonitordSink} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class MonitordSinkTest {

    @AfterClass
    public static void afterClass() {
        ACTOR_SYSTEM.terminate();
    }

    @Before
    public void before() {
        _monitordSinkBuilder = new MonitordSink.Builder()
                .setName("monitord_sink_test")
                .setActorSystem(ACTOR_SYSTEM)
                .setUri(URI.create("http://localhost:8888"))
                .setPeriodicMetrics(Mockito.mock(PeriodicMetrics.class));
    }

    @Test
    public void testSerializeMerge() {
        final String service = "service-testSerializeMerge";
        final String metric = "metric-testSerializeMerge";
        final Duration period = Duration.ofMinutes(5);
        final String host = "test-host";
        final String cluster = "test-cluster";
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service)
                                .setCluster(cluster)
                                .setMetric(metric)
                                .build())
                        .setPeriod(period)
                        .setHost(host)
                        .build(),
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service)
                                .setCluster(cluster)
                                .setMetric(metric)
                                .build())
                        .setPeriod(period)
                        .setHost(host)
                        .build());
        final MonitordSink monitordSink = _monitordSinkBuilder.build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .build();
        final Collection<HttpPostSink.SerializedDatum> results = monitordSink.serialize(periodicData);
        Assert.assertEquals(1, results.size());
    }

    @Test
    public void testSerializeNoMergeService() {
        final String service = "service-testSerializeNoMergeService";
        final String metric = "metric-testSerializeNoMergeService";
        final Duration period = Duration.ofMinutes(5);
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service + "-1")
                                .setMetric(metric)
                                .build())
                        .setPeriod(period)
                        .build(),
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service + "-2")
                                .setMetric(metric)
                                .build())
                        .setPeriod(period)
                        .build());
        final MonitordSink monitordSink = _monitordSinkBuilder.build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .build();
        final Collection<HttpPostSink.SerializedDatum> results = monitordSink.serialize(periodicData);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testSerializeNoMergeMetric() {
        final String service = "service-testSerializeNoMergeMetric";
        final String metric = "metric-testSerializeNoMergeMetric";
        final Duration period = Duration.ofMinutes(5);
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service)
                                .setMetric(metric + "-1")
                                .build())
                        .setPeriod(period)
                        .build(),
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service)
                                .setMetric(metric + "-2")
                                .build())
                        .setPeriod(period)
                        .build());
        final MonitordSink monitordSink = _monitordSinkBuilder.build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .build();
        final Collection<HttpPostSink.SerializedDatum> results = monitordSink.serialize(periodicData);
        Assert.assertEquals(2, results.size());
    }

    @Test
    public void testSerializeNoMergePeriod() {
        final String service = "service-testSerializeNoMergePeriod";
        final String metric = "metric-testSerializeNoMergePeriod";
        final ImmutableList<AggregatedData> data = ImmutableList.of(
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service)
                                .setMetric(metric)
                                .build())
                        .setPeriod(Duration.ofMinutes(5))
                        .build(),
                TestBeanFactory.createAggregatedDataBuilder()
                        .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                .setService(service)
                                .setMetric(metric)
                                .build())
                        .setPeriod(Duration.ofMinutes(1))
                        .build());
        final MonitordSink monitordSink = _monitordSinkBuilder.build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setData(data)
                .build();
        final Collection<HttpPostSink.SerializedDatum> results = monitordSink.serialize(periodicData);
        Assert.assertEquals(2, results.size());
    }

    private MonitordSink.Builder _monitordSinkBuilder;
    private static final ActorSystem ACTOR_SYSTEM = ActorSystem.apply();
}
