/*
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

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.Condition;
import com.arpnetworking.tsdcore.model.FQDSN;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.model.Quantity;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.arpnetworking.utility.BaseActorTest;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RequestPatternBuilder;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import net.sf.oval.exception.ConstraintsViolatedException;
import org.apache.pekko.http.javadsl.model.MediaTypes;
import org.awaitility.Awaitility;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.net.URI;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link KairosDbSink} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class KairosDbSinkTest extends BaseActorTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        _wireMockServer = new WireMockServer(0);
        _wireMockServer.start();
        _wireMock = new WireMock(_wireMockServer.port());
        _kairosDbSinkBuilder = new KairosDbSink.Builder()
                .setName("kairosdb_sink_test")
                .setActorSystem(getSystem())
                .setUri(URI.create("http://localhost:" + _wireMockServer.port() + PATH))
                .setPeriodicMetrics(Mockito.mock(PeriodicMetrics.class));
    }

    @After
    @Override
    public void tearDown() {
        super.tearDown();
        _wireMockServer.stop();
    }

    @Test(expected = ConstraintsViolatedException.class)
    public void testNegativeBaseBackOff() {
        _kairosDbSinkBuilder.setBaseBackoff(Duration.ofMillis(-5)).build();
    }

    @Test
    public void testPost() throws InterruptedException, IOException {
        // Fake a successful post to KairosDb
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(200)));

        // Post data to KairosDb
        _kairosDbSinkBuilder.build().recordAggregateData(createPeriodicData(10L));
        // Allow the request/response to complete
        Thread.sleep(3000);

        // Request matcher
        final RequestPatternBuilder requestPattern = WireMock.postRequestedFor(WireMock.urlEqualTo(PATH))
                .withHeader("Content-Type", WireMock.equalTo(MediaTypes.APPLICATION_JSON.toString()));

        // Assert that data was sent
        _wireMock.verifyThat(1, requestPattern);

        // Compare the bodies
        final JsonNode actual = OBJECT_MAPPER.readTree(_wireMock.find(requestPattern).get(0).getBody());
        final JsonNode expected = OBJECT_MAPPER.readTree(getClass().getResource(getClass().getSimpleName() + ".testPost.expected.json")
                .openStream());
        Assert.assertEquals(expected, actual);
    }

    @Test
    public void testPostFailure() {
         // Fake a failing post to KairosDb
        _wireMock.register(WireMock.post(WireMock.urlEqualTo(PATH))
                .willReturn(WireMock.aResponse()
                        .withStatus(502)));
        _kairosDbSinkBuilder.setMaximumAttempts(2).setBaseBackoff(Duration.ofMillis(1)).build()
                .recordAggregateData(createPeriodicData(10L));

        Awaitility.await().atMost(10, TimeUnit.SECONDS).untilAsserted(
                () -> _wireMock.verifyThat(2, WireMock.postRequestedFor(WireMock.urlEqualTo(PATH)))
        );
     }


    private KairosDbSink.Builder _kairosDbSinkBuilder;
    private WireMockServer _wireMockServer;
    private WireMock _wireMock;

    private static final String PATH = "/kairos/post/path";
    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final ObjectMapper OBJECT_MAPPER = ObjectMapperFactory.getInstance();

    private PeriodicData createPeriodicData(final long populationSize) {
        final ZonedDateTime start = ZonedDateTime.ofInstant(
                Instant.ofEpochMilli(1457768160000L),
                ZoneOffset.UTC);
        final FQDSN fqdsn = new FQDSN.Builder()
                .setCluster("MyCluster")
                .setMetric("MyMetric")
                .setService("MyService")
                .setStatistic(STATISTIC_FACTORY.getStatistic("max"))
                .build();
        return new PeriodicData.Builder()
                .setConditions(ImmutableList.of(
                        new Condition.Builder()
                                .setFQDSN(fqdsn)
                                .setName("critical")
                                .setThreshold(new Quantity.Builder()
                                        .setValue(2.46)
                                        .build())
                                .setTriggered(true)
                                .build()))
                .setData(ImmutableList.of(
                        new AggregatedData.Builder()
                                .setFQDSN(fqdsn)
                                .setHost("MyHost")
                                .setIsSpecified(true)
                                .setPeriod(Duration.ofMinutes(1))
                                .setPopulationSize(populationSize)
                                .setStart(start)
                                .setValue(new Quantity.Builder()
                                        .setValue(1.23)
                                        .build())
                                .build()))
                .setDimensions(ImmutableMap.of(
                        "host", "myhost.example.com",
                        "domain", "example.com"))
                .setPeriod(Duration.ofMinutes(1))
                .setStart(start)
                .build();
    }
}
