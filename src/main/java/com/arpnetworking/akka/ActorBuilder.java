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

import akka.actor.Props;
import com.arpnetworking.commons.builder.OvalBuilder;

import java.util.function.Function;

/**
 * Builder for actors.
 *
 * @param <B> The type of the builder
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public abstract class ActorBuilder<B extends ActorBuilder<B>> extends OvalBuilder<Props> {
    /**
     * Protected constructor.
     *
     * @param createProps method to create a {@link Props} from the {@link ActorBuilder}
     */
    protected ActorBuilder(final Function<B, Props> createProps) {
        super(createProps);
    }

    /**
     * Called by setters to always return appropriate subclass of
     * {@link ActorBuilder}, even from setters of base class.
     *
     * @return instance with correct {@link ActorBuilder} class type.
     */
    protected abstract B self();
}
