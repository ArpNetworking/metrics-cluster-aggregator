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
package com.arpnetworking.configuration.jackson.akka;

import akka.actor.Actor;
import com.arpnetworking.akka.ActorBuilder;
import com.arpnetworking.commons.builder.Builder;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.TreeNode;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.TextNode;

import java.io.IOException;

/**
 * Deserializer that will create an ActorBuilder for the given actor, then create a Props from Guice.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ActorBuilderDeserializer extends JsonDeserializer<ActorBuilder<?, ?>> {
    /**
     * Public constructor.
     *
     * @param mapper the {@link ObjectMapper} to use to deserialize the {@link Builder}
     */
    public ActorBuilderDeserializer(final ObjectMapper mapper) {
        _mapper = mapper;
    }

    @Override
    public ActorBuilder<? extends ActorBuilder<?, ?>, ? extends Actor> deserialize(final JsonParser p, final DeserializationContext ctxt)
            throws IOException {
        final TreeNode treeNode = p.readValueAsTree();
        final String type = ((TextNode) treeNode.get("type")).textValue();
        try {
            final Class<?> clazz = Class.forName(type);
            final Class<? extends ActorBuilder<? extends ActorBuilder<?, ?>, ? extends Actor>> builder = getBuilderForClass(clazz);
            return _mapper.readValue(treeNode.traverse(), builder);
        } catch (final ClassNotFoundException e) {
            throw new JsonMappingException(p, String.format("Unable to find class %s referenced by Props type", type));
        }
    }

    @SuppressWarnings("unchecked")
    private static Class<? extends ActorBuilder<? extends ActorBuilder<?, ?>, ? extends Actor>> getBuilderForClass(final Class<?> clazz)
            throws ClassNotFoundException {
        return (Class<? extends ActorBuilder<? extends ActorBuilder<?, ?>, ? extends Actor>>) (Class.forName(
                clazz.getName() + "$Builder",
                true, // initialize
                clazz.getClassLoader()));
    }

    private final ObjectMapper _mapper;
}
