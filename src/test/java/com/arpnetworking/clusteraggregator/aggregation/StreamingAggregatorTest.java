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
package com.arpnetworking.clusteraggregator.aggregation;

import akka.actor.ActorRef;
import akka.actor.ReceiveTimeout;
import akka.actor.Terminated;
import akka.cluster.sharding.ShardRegion;
import akka.testkit.TestActorRef;
import akka.testkit.TestProbe;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.utility.BaseActorTest;
import com.google.common.collect.ImmutableSet;
import org.junit.Assert;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link StreamingAggregator} actor.
 *
 * @author Christian Briones (cbriones at dropbox dot com)
 */
public class StreamingAggregatorTest extends BaseActorTest {

    @Test
    public void passivatesProperlyWhenLivelinessTimeoutIsExceeded() {
        final TestProbe probe = TestProbe.apply(getSystem());
        final ActorRef aggregator = createAggregator(probe);
        probe.watch(aggregator);

        // We expect a passivate message after at least LIVELINESS_TIMEOUT_SECS
        final ShardRegion.Passivate passivate = probe.expectMsgClass(PROBE_TIMEOUT, ShardRegion.Passivate.class);
        aggregator.tell(passivate.stopMessage(), aggregator);
        final Terminated terminated = probe.expectMsgClass(Terminated.class);
        Assert.assertEquals(aggregator, terminated.getActor());
    }

    @Test
    public void dataKeepsActorLive() {
        final TestProbe probe = TestProbe.apply(getSystem());
        final ActorRef aggregator = createAggregator(probe);
        probe.watch(aggregator);

        final Messages.StatisticSetRecord record = Messages.StatisticSetRecord.newBuilder()
                .setMetric("my_metric")
                .setPeriod("PT1M")
                .setPeriodStart("2020-12-10T19:00:00Z")
                .build();

        aggregator.tell(record, probe.ref());
        probe.expectNoMessage(FiniteDuration.apply(LIVELINESS_TIMEOUT_SEC, TimeUnit.SECONDS));

        aggregator.tell(ReceiveTimeout.getInstance(), aggregator);
        final ShardRegion.Passivate passivate = probe.expectMsgClass(PROBE_TIMEOUT, ShardRegion.Passivate.class);
        aggregator.tell(passivate.stopMessage(), aggregator);
        final Terminated terminated = probe.expectMsgClass(Terminated.class);
        Assert.assertEquals(aggregator, terminated.getActor());
    }

    public ActorRef createAggregator(final TestProbe probe) {
        final TestProbe ignored = TestProbe.apply(getSystem());
        return TestActorRef.apply(
                StreamingAggregator.props(
                        ignored.ref(),
                        ignored.ref(),
                        "",
                        ImmutableSet.of(),
                        true,
                        Duration.ofSeconds(5),
                        Optional.of(Duration.ofSeconds(LIVELINESS_TIMEOUT_SEC))),
        probe.ref(),
                "agg",
                getSystem());
    }

    private static final int LIVELINESS_TIMEOUT_SEC = 5;
    private static final FiniteDuration PROBE_TIMEOUT = FiniteDuration.apply(10, TimeUnit.SECONDS);
}
