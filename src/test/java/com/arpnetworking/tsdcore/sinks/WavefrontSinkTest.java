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
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.arpnetworking.utility.BaseActorTest;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.verification.LoggedRequest;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.protobuf.InvalidProtocolBufferException;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.net.URI;
import java.util.List;

/**
 * Tests for the <code>WavefrontSink</code> class.
 *
 * @author Gil Markham (gil at groupon dot com)
 */
public class WavefrontSinkTest extends BaseActorTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        _wireMockServer = new WireMockServer(0);
        _wireMockServer.start();
        _wireMock = new WireMock(_wireMockServer.port());
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
        _wireMockServer.stop();
    }

    @Test
    public void testWavefrontSink() throws InvalidProtocolBufferException, InterruptedException {
        _wireMock.register(WireMock.post(WireMock.urlMatching("/api/daemon/[^/]*/checkin"))
                .withQueryParam("token", WireMock.equalTo("MyApiToken"))
                .willReturn(
                        WireMock.aResponse()
                                .withBody("")
                                .withHeader("Content-Type", "text/html")
                                .withStatus(200)));

        final Sink sink = new WavefrontSink.Builder()
                .setName("Wavefront")
                .setToken("MyApiToken")
                .setFlushThreads(1)
                .setIdFile("/tmp/idFile")
                .setUri(URI.create("http://localhost:" + _wireMockServer.port() + "/api"))
                .setBufferBasename("target/buffer")
                .build();

        final DateTime start = DateTime.now();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicDataBuilder()
                .setPeriod(Period.seconds(1))
                .setStart(start)
                .setDimensions(ImmutableMap.of("host", "app1.example.com"))
                .setData(
                        ImmutableList.of(
                                TestBeanFactory.createAggregatedDataBuilder()
                                        .setFQDSN(
                                                TestBeanFactory.createFQDSNBuilder()
                                                        .setCluster("MyCluster")
                                                        .setService("MyService")
                                                        .setMetric("MyMetric")
                                                        .setStatistic(STATISTICS_FACTORY.getStatistic("tp0"))
                                                        .build())
                                        .setValue(new Quantity.Builder().setValue(1.23).build())
                                        .build()))
                .build();
        sink.recordAggregateData(periodicData);

        Thread.sleep(1000);

        _wireMock.verifyThat(WireMock.postRequestedFor(WireMock.urlMatching("/api/daemon/[^/]*/pushdata/.*"))
                .withHeader("Content-Type", WireMock.equalTo("text/plain"))
                .withRequestBody(WireMock.matching("\"MyService\\.PT1S\\.MyMetric.min\" .*")));

        // Parse post request body
        final List<LoggedRequest> requests = _wireMock.find(WireMock.postRequestedFor(
                WireMock.urlMatching("/api/daemon/[^/]*/pushdata/.*")));
        Assert.assertEquals(1, requests.size());

    }

    private WireMockServer _wireMockServer;
    private WireMock _wireMock;

    private static final StatisticFactory STATISTICS_FACTORY = new StatisticFactory();
}
