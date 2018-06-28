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
package com.arpnetworking.guice.akka;

import akka.actor.Actor;
import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.actor.IndirectActorProducer;
import akka.actor.Props;
import com.arpnetworking.logback.annotations.LogValue;
import com.arpnetworking.steno.LogValueMapFactory;
import com.google.inject.Inject;
import com.google.inject.Injector;
import com.google.inject.Provider;

/**
 * A Guice-based factory for Akka actors.
 *
 * TODO(vkoskela): This is _duplicated_ in metrics-portal and should find its way to a common utility package.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class GuiceActorCreator implements IndirectActorProducer {
    /**
     * Creates a <code>Props</code> for this creator.
     * @param injector the Guice injector to create actors from
     * @param clazz the class to create
     * @return a new <code>Props</code>
     */
    public static Props props(final Injector injector, final Class<? extends Actor> clazz) {
        return Props.create(GuiceActorCreator.class, injector, clazz);
    }

    /**
     * Creates a provider that is suitable for eager singleton binding.
     *
     * @param clazz the class to create
     * @param name the name of the actor
     * @return a {@link Provider} that will create the actor
     */
    public static Provider<ActorRef> provider(final Class<? extends Actor> clazz, final String name) {
        return new Provider<ActorRef>() {
            @Override
            public ActorRef get() {
                return _system.actorOf(props(_injector, clazz), name);
            }

            @Inject
            private ActorSystem _system;

            @Inject
            private Injector _injector;
        };
    }

    /**
     * Public constructor.
     *
     * @param injector <code>The Guice injector to use to construct the actor</code>.
     * @param clazz Class to create.
     */
    public GuiceActorCreator(final Injector injector, final Class<? extends Actor> clazz) {
        _injector = injector;
        _clazz = clazz;
    }

    @Override
    public Actor produce() {
        return _injector.getInstance(_clazz);
    }

    @Override
    public Class<? extends Actor> actorClass() {
        return _clazz;
    }

    /**
     * Generate a Steno log compatible representation.
     *
     * @return Steno log compatible representation.
     */
    @LogValue
    public Object toLogValue() {
        return LogValueMapFactory.builder(this)
                .put("injector", _injector)
                .put("class", _clazz)
                .build();
    }

    @Override
    public String toString() {
        return toLogValue().toString();
    }

    private final Injector _injector;
    private final Class<? extends Actor> _clazz;

}
