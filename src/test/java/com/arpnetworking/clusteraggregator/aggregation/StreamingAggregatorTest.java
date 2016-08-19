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
package com.arpnetworking.clusteraggregator.aggregation;

import akka.actor.PoisonPill;
import akka.pattern.Patterns;
import akka.testkit.JavaTestKit;
import akka.testkit.TestActorRef;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.utility.BaseActorTest;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Test;
import scala.concurrent.Await;
import scala.concurrent.Future;
import scala.concurrent.duration.Duration;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Test for <link>StreamingAggregator<link>.
 *
 * @author Matthew Hayter (mhayter at groupon dot com)
 */
@SuppressWarnings("checkstyle:methodlength") // Parent pom should be changed to not apply to tests.
public class StreamingAggregatorTest extends BaseActorTest {

    @Test
    public void testAggregateDimensions() throws Exception {
        final JavaTestKit mockEmitter = new JavaTestKit(getSystem());
        final JavaTestKit mockPeriodStatsListener = new JavaTestKit(getSystem());
        final JavaTestKit mockBookkeeper = new JavaTestKit(getSystem());
        // Create actor
        final TestActorRef<StreamingAggregator> streamingAggregator =
                TestActorRef.create(
                        getSystem(),
                        StreamingAggregator.props(
                                mockBookkeeper.getRef(),
                                mockPeriodStatsListener.getRef(),
                                mockEmitter.getRef(),
                                ".testcolo"));

        // Push say 4 messages; two pairs with different dimensions. Get the aggregates out.
        final String metricName = "myMetric";
        final String clusterName = "mycluster";
        final String serviceName = "myservice";

        // A1 should be aggregated with A2
        final Messages.StatisticSetRecord statsA1 = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(Period.minutes(1).toString())
                .setPeriodStart(DateTime.now().minuteOfDay().roundFloorCopy().toString())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim1").setValue("val1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim2").setValue("val1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("host").setValue("myhost1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("service").setValue(serviceName).build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("cluster").setValue(clusterName).build())
                .setCluster(clusterName)
                .setService(serviceName)
                .addStatistics(Messages.StatisticRecord.newBuilder()
                        .setValue(4)
                        .setStatistic("sum")
                        .setUserSpecified(false)
                        .setUnit("")
                        .build())
                .build();

        final Messages.StatisticSetRecord statsA2 = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(Period.minutes(1).toString())
                .setPeriodStart(DateTime.now().minuteOfDay().roundFloorCopy().toString())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim1").setValue("val1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim2").setValue("val1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("host").setValue("myhost2").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("service").setValue(serviceName).build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("cluster").setValue(clusterName).build())
                .setCluster(clusterName)
                .setService(serviceName)
                .addStatistics(Messages.StatisticRecord.newBuilder()
                        .setValue(100)
                        .setStatistic("sum")
                        .setUserSpecified(false)
                        .setUnit("")
                        .build())
                .build();

        // B1 should be aggregated with B2
        final Messages.StatisticSetRecord statsB1 = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(Period.minutes(1).toString())
                .setPeriodStart(DateTime.now().minuteOfDay().roundFloorCopy().toString())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim1").setValue("val1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim2").setValue("otherval").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("host").setValue("myhost3").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("service").setValue(serviceName).build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("cluster").setValue(clusterName).build())
                .setCluster(clusterName)
                .setService(serviceName)
                .addStatistics(Messages.StatisticRecord.newBuilder()
                        .setValue(200)
                        .setStatistic("sum")
                        .setUserSpecified(false)
                        .setUnit("")
                        .build())
                .build();

        final Messages.StatisticSetRecord statsB2 = Messages.StatisticSetRecord.newBuilder()
                .setMetric(metricName)
                .setPeriod(Period.minutes(1).toString())
                .setPeriodStart(DateTime.now().minuteOfDay().roundFloorCopy().toString())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim1").setValue("val1").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("dim2").setValue("otherval").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("host").setValue("myhost4").build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("service").setValue(serviceName).build())
                .addDimensions(Messages.DimensionEntry.newBuilder().setKey("cluster").setValue(clusterName).build())
                .setCluster(clusterName)
                .setService(serviceName)
                .addStatistics(Messages.StatisticRecord.newBuilder()
                        .setValue(15)
                        .setStatistic("sum")
                        .setUserSpecified(false)
                        .setUnit("")
                        .build())
                .build();

        // Freeze time
        final long testStartTime = DateTimeUtils.currentTimeMillis();
        DateTimeUtils.setCurrentMillisFixed(testStartTime);

        streamingAggregator.receive(statsA1);
        streamingAggregator.receive(statsA2);
        streamingAggregator.receive(statsB1);
        streamingAggregator.receive(statsB2);

        // Bump the time forward 120 seconds to trigger a bucket check and bucket close & emit.
        DateTimeUtils.setCurrentMillisFixed(testStartTime + 120 * 1000);

        // Wait for the emit.
        final PeriodicData msg1 = (PeriodicData) mockEmitter.expectMsgAnyClassOf(Duration.create(6, TimeUnit.SECONDS), PeriodicData.class);
        final PeriodicData msg2 = (PeriodicData) mockEmitter.expectMsgAnyClassOf(Duration.create(6, TimeUnit.SECONDS), PeriodicData.class);

        // Check the dimensions.
        Assert.assertEquals("val1", msg1.getDimensions().get("dim1"));
        Assert.assertEquals("val1", msg2.getDimensions().get("dim1"));
        final List<String> dim2Vals = Arrays.asList(msg1.getDimensions().get("dim2"), msg2.getDimensions().get("dim2"));
        Assert.assertTrue((dim2Vals.get(0).equals("val1") && dim2Vals.get(1).equals("otherval"))
                || (dim2Vals.get(0).equals("otherval") && dim2Vals.get(1).equals("val1"))
        );

        // Wait for shutdown.
        final Future<Boolean> gracefulStop =
                Patterns.gracefulStop(streamingAggregator, Duration.create(5, TimeUnit.SECONDS), PoisonPill.getInstance());
        Await.result(gracefulStop, Duration.create(6, TimeUnit.SECONDS));

        // Nothing else was emitted.
        mockEmitter.expectNoMsg();

        DateTimeUtils.setCurrentMillisSystem();
    }
}
