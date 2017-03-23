/**
 * Copyright 2016 InscopeMetrics, Inc
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

import akka.actor.ActorRef;
import akka.actor.PoisonPill;
import akka.actor.Props;
import com.arpnetworking.akka.ActorBuilder;
import com.arpnetworking.akka.NonJoiningClusterJoiner;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.configuration.jackson.akka.ActorBuilderDeserializer;
import com.arpnetworking.utility.BaseActorTest;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import org.intellij.lang.annotations.Language;
import org.junit.Test;

import java.io.IOException;

/**
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ActorBuilderTest extends BaseActorTest {
    @Test
    public void testBuild() {
        final NonJoiningClusterJoiner.Builder builder = new NonJoiningClusterJoiner.Builder();

        final ActorRef ref = getSystem().actorOf(Props.create(builder));
        ref.tell(PoisonPill.getInstance(), ActorRef.noSender());
    }

    @Test
    public void testPolyDeserialize() throws IOException {
        final ObjectMapper mapper = ObjectMapperFactory.createInstance();
        final SimpleModule module = new SimpleModule();
        module.addDeserializer(ActorBuilder.class, new ActorBuilderDeserializer(mapper));
        mapper.registerModule(module);

        @Language("JSON") final String data = "{\n"
                + "  \"type\": \"com.arpnetworking.akka.NonJoiningClusterJoiner\"\n"
                + "}";
        final ActorBuilder<?, ?> builder = mapper.readValue(data, ActorBuilder.class);
        final ActorRef ref = getSystem().actorOf(Props.create(builder));
        ref.tell(PoisonPill.getInstance(), ActorRef.noSender());
    }
}
