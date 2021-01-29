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
package com.arpnetworking.clusteraggregator.http;

import akka.http.javadsl.model.HttpRequest;
import com.arpnetworking.metrics.incubator.PeriodicMetrics;
import com.arpnetworking.utility.BaseActorTest;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;

/**
 * Tests for the {@code Routes} class.
 *
 * @author Qinyan Li (lqy520s at hotmail dot com)
 */
public class RoutesTest extends BaseActorTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        _routes = new Routes(getSystem(), _mockPeriodicMetrics, HEALTH_CHECK_PATH, STATUS_PATH, VERSION_PATH);
    }

    @Test
    public void testApply() throws InterruptedException {
        final HttpRequest request = HttpRequest.create("test_health_check");
        _routes.apply(request);

        // wait for the request to complete. The actor time out is 5 seconds so here wait for 6 seconds.
        Thread.sleep(6000);

        Mockito.verify(_mockPeriodicMetrics).recordTimer(
                Mockito.eq("rest_service/GET/test_health_check/request"),
                Mockito.anyLong(),
                Mockito.any());
        Mockito.verify(_mockPeriodicMetrics).recordGauge(
                Mockito.eq("rest_service/GET/test_health_check/body_size"),
                Mockito.anyLong());
        Mockito.verify(_mockPeriodicMetrics).recordCounter(
                "rest_service/GET/test_health_check/status/5xx",
                1);

        final HttpRequest unknownRequest = HttpRequest.create("kb23k1dnlkns02");
        _routes.apply(unknownRequest);

        // wait for the request to complete. Since unknown_route will not call actor, don't have to wait for 6 seconds. 
        Thread.sleep(1000);

        Mockito.verify(_mockPeriodicMetrics).recordTimer(
                Mockito.eq("rest_service/GET/unknown_route/request"),
                Mockito.anyLong(),
                Mockito.any());
        Mockito.verify(_mockPeriodicMetrics).recordGauge(
                Mockito.eq("rest_service/GET/unknown_route/body_size"),
                Mockito.anyLong());
        Mockito.verify(_mockPeriodicMetrics).recordCounter(
                "rest_service/GET/unknown_route/status/4xx",
                1);
    }

    private Routes _routes;

    private static final String HEALTH_CHECK_PATH = "test_health_check";
    private static final String STATUS_PATH = "test_status";
    private static final String VERSION_PATH = "test_version";

    @Mock
    private PeriodicMetrics _mockPeriodicMetrics;
}
