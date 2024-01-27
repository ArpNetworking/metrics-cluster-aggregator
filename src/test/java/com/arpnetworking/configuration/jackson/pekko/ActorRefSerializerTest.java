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
package com.arpnetworking.configuration.jackson.pekko;

import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.utility.BaseActorTest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Actor;
import org.apache.pekko.actor.ActorRef;
import org.apache.pekko.actor.ActorSystem;
import org.apache.pekko.actor.Props;
import org.apache.pekko.testkit.TestActorRef;
import org.apache.pekko.testkit.TestProbe;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import scala.concurrent.duration.FiniteDuration;

import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Tests for the pekko actor reference serializer.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ActorRefSerializerTest extends BaseActorTest {

    @Before
    @Override
    public void setUp() {
        super.setUp();
        _mapper = ObjectMapperFactory.createInstance();
        _mapper.registerModule(new PekkoModule(getSystem()));
    }

    @Test
    public void testSerializesToString() throws JsonProcessingException {
        final TestActorRef<Actor> ref = TestActorRef.create(getSystem(), Props.create(DoNothingActor.class));

        final JsonNode serialized = _mapper.valueToTree(ref);
        Assert.assertTrue(serialized.isTextual());
    }

    @Test
    public void testSerializeDeserialize() throws IOException {
        final TestProbe probe = TestProbe.apply(getSystem());
        final JsonNode serialized = _mapper.valueToTree(probe.ref());

        final ActorSystem system2 = ActorSystem.create("Test");
        final ObjectMapper system2Mapper = ObjectMapperFactory.createInstance();
        system2Mapper.registerModule(new PekkoModule(system2));
        final ActorRef remoteRef = system2Mapper.readValue(serialized.toString(), ActorRef.class);

        remoteRef.tell("OK", ActorRef.noSender());
        probe.expectMsg(FiniteDuration.apply(3L, TimeUnit.SECONDS), "OK");
    }

    private ObjectMapper _mapper;

    private static final class DoNothingActor extends AbstractActor {
        @Override
        public Receive createReceive() {
            return receiveBuilder()
                    .matchAny(any -> { })
                    .build();
        }
    }
}
