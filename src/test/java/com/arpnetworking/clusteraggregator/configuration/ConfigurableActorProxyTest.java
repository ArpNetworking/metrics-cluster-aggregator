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
package com.arpnetworking.clusteraggregator.configuration;

import com.arpnetworking.utility.BaseActorTest;
import com.arpnetworking.utility.ConfiguredLaunchableFactory;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Props;
import org.apache.pekko.actor.Terminated;
import org.apache.pekko.testkit.CallingThreadDispatcher;
import org.apache.pekko.testkit.TestActorRef;
import org.apache.pekko.testkit.TestProbe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link com.arpnetworking.clusteraggregator.configuration.ConfigurableActorProxy} class.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ConfigurableActorProxyTest extends BaseActorTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        Mockito.when(_factoryMock.create(Mockito.any())).thenReturn(Props.create(NullActor.class));
    }

    @Test
    public void propsCreation() {
        final Props props = ConfigurableActorProxy.props(t -> null);
        Assert.assertNotNull(props);
    }

    @Test
    public void testInitialActorStart() {
        // Verifies that the initial actor is created via the factory when the first ApplyConfiguration message is sent
        final TestActorRef<ConfigurableActorProxy<?>> ref = TestActorRef.create(
                getSystem(),
                mockProps().withDispatcher(CallingThreadDispatcher.Id()));
        final Object dummyConfig = new Object();
        ref.tell(new ConfigurableActorProxy.ApplyConfiguration<>(dummyConfig), ActorRef.noSender());
        Mockito.verify(_factoryMock).create(Mockito.same(dummyConfig));
    }

    @Test
    public void testTeardownAndSwap() {
        final TestActorRef<ConfigurableActorProxy<?>> ref = TestActorRef.create(
                getSystem(),
                mockProps().withDispatcher(CallingThreadDispatcher.Id()));
        final TestProbe probe = TestProbe.apply(getSystem());
        ref.tell(new ConfigurableActorProxy.SubscribeToNotifications(), probe.ref());
        final Object dummyConfig = new Object();
        ref.tell(new ConfigurableActorProxy.ApplyConfiguration<>(dummyConfig), ActorRef.noSender());
        // Make sure we start the first one
        Mockito.verify(_factoryMock).create(Mockito.same(dummyConfig));
        final ActorRef firstChild = probe.expectMsgClass(
                FiniteDuration.apply(2000, TimeUnit.MILLISECONDS),
                ConfigurableActorProxy.ConfigurableActorStarted.class).getActor();

        final Object newConfig = new Object();

        final TestProbe deathwatchProbe = TestProbe.apply(getSystem());
        deathwatchProbe.watch(firstChild);

        // Swap the config to a new one
        ref.tell(new ConfigurableActorProxy.ApplyConfiguration<>(newConfig), ActorRef.noSender());

        // Make sure the first actor dies
        deathwatchProbe.expectMsgClass(FiniteDuration.apply(2000, TimeUnit.MILLISECONDS), Terminated.class);

        // NOTE: lifecycle monitoring does not execute in the CallingThreadDispatcher as expected.
        // There is an arbitrary 2000ms wait for the termination message to be dispatched
        Mockito.verify(_factoryMock, Mockito.timeout(2000)).create(Mockito.same(newConfig));
        final ActorRef started = probe.expectMsgClass(
                FiniteDuration.apply(2000, TimeUnit.MILLISECONDS),
                ConfigurableActorProxy.ConfigurableActorStarted.class).getActor();

        // Make sure that a new actor was started from the config
        Assert.assertNotEquals(firstChild, started);
    }

    private Props mockProps() {
        return ConfigurableActorProxy.props(_factoryMock);
    }

    @Mock
    private ConfiguredLaunchableFactory<Props, Object> _factoryMock;

    private static final class NullActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchAny(message -> { })
                    .build();
        }
    }
}
