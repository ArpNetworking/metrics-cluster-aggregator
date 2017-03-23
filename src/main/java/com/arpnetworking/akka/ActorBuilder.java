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
package com.arpnetworking.akka;

import akka.actor.Actor;
import akka.japi.Creator;
import com.arpnetworking.commons.builder.OvalBuilder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.util.function.Function;

/**
 * Builder for actors.
 *
 * @param <B> The type of the builder
 * @param <S> type of the object to be built
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
@SuppressFBWarnings("SE_NO_SUITABLE_CONSTRUCTOR")
public abstract class ActorBuilder<B extends ActorBuilder<B, S>, S extends Actor> extends OvalBuilder<S> implements Creator<S> {
    /**
     * Protected constructor for subclasses.
     *
     * @param targetConstructor The constructor for the concrete type to be created by this builder.
     */
    protected ActorBuilder(final Function<B, S> targetConstructor) {
        super(targetConstructor);
    }

    /**
     * Called by setters to always return appropriate subclass of
     * {@link ActorBuilder}, even from setters of base class.
     *
     * @return instance with correct {@link ActorBuilder} class type.
     */
    protected abstract B self();

    /**
     * {@inheritDoc}
     */
    @Override
    public S create() throws Exception {
        return build();
    }

    private static final long serialVersionUID = 1L;
}
