/**
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
package com.arpnetworking.configuration.jackson.akka;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.fasterxml.jackson.databind.module.SimpleModule;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

/**
 * Jackson module for serializing and deserializing Akka objects.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class AkkaModule extends SimpleModule {

    /**
     * Public constructor.
     *
     * @param system the actor system to resolve references
     */
    public AkkaModule(final ActorSystem system) {
        _system = system;
    }

    @Override
    public void setupModule(final SetupContext context) {
        addSerializer(ActorRef.class, new ActorRefSerializer(_system));
        addDeserializer(ActorRef.class, new ActorRefDeserializer(_system));
        super.setupModule(context);
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("actorSystem", _system)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    @SuppressFBWarnings("SE_BAD_FIELD")
    private final ActorSystem _system;

    private static final long serialVersionUID = 4294591813352245070L;
}
