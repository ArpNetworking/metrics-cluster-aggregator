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

package com.arpnetworking.clusteraggregator;

import com.arpnetworking.utility.BaseActorTest;
import org.apache.pekko.actor.Actor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.Address;
import org.apache.pekko.actor.UnhandledMessage;
import org.apache.pekko.cluster.Cluster;
import org.apache.pekko.cluster.ClusterEvent;
import org.apache.pekko.cluster.ClusterReadView;
import org.apache.pekko.cluster.Member;
import org.apache.pekko.cluster.MemberStatus;
import org.apache.pekko.cluster.UniqueAddress;
import org.apache.pekko.testkit.TestActorRef;
import org.apache.pekko.testkit.TestProbe;
import org.apache.pekko.util.Version;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import scala.Option;
import scala.collection.immutable.HashMap;
import scala.collection.immutable.HashSet;
import scala.collection.immutable.Set;
import scala.collection.immutable.Set$;
import scala.collection.immutable.TreeSet;
import scala.concurrent.duration.FiniteDuration;

import java.util.concurrent.TimeUnit;

/**
 * Tests for the {@link Status} actor.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class StatusTest extends BaseActorTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        final Address localAddress = Address.apply("tcp", "default");
        final Member selfMember = new Member(UniqueAddress.apply(localAddress, 1821L),
                                             1,
                                             MemberStatus.up(),
                                             new Set.Set1<>("test"),
                                             Version.Zero());
        Mockito.when(_clusterMock.selfAddress()).thenReturn(localAddress);
        final ClusterReadView readView = Mockito.mock(ClusterReadView.class);
        Mockito.when(readView.self()).thenReturn(selfMember);
        Mockito.when(_clusterMock.readView()).thenReturn(readView);
        final TreeSet<Member> memberTreeSet = new TreeSet<>(Member.ordering());
        memberTreeSet.$plus(selfMember);
        final ClusterEvent.CurrentClusterState state = new ClusterEvent.CurrentClusterState(
                memberTreeSet,
                Member.none(),
                new HashSet<>(),
                Option.empty(),
                new HashMap<>(),
                Set$.MODULE$.empty(),
                Set$.MODULE$.empty());
        Mockito.doAnswer(
                invocation -> {
                    final ActorRef ref = (ActorRef) invocation.getArguments()[0];
                    ref.tell(state, ActorRef.noSender());
                    return null;
                })
                .when(_clusterMock).sendCurrentClusterState(Mockito.any());
    }

    @Test
    public void createProps() {
        TestActorRef.create(getSystem(), Status.props(_clusterMock, _listenerProbe.ref(), _listenerProbe.ref()));
    }

    @Test
    public void doesNotSwallowUnhandled() {
        final TestProbe probe = TestProbe.apply(getSystem());
        final TestActorRef<Actor> ref = TestActorRef.create(
                getSystem(),
                Status.props(_clusterMock, _listenerProbe.ref(), _listenerProbe.ref()));
        getSystem().eventStream().subscribe(probe.ref(), UnhandledMessage.class);
        ref.tell("notAValidMessage", ActorRef.noSender());
        probe.expectMsgClass(FiniteDuration.apply(3, TimeUnit.SECONDS), UnhandledMessage.class);
    }

    @Mock
    private TestProbe _listenerProbe;

    @Mock
    private Cluster _clusterMock;
}

