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

package com.arpnetworking.utility;

import com.arpnetworking.clusteraggregator.configuration.ConfigurableActorProxy;
import com.arpnetworking.configuration.Configuration;
import com.arpnetworking.configuration.ConfigurationException;
import com.arpnetworking.configuration.Listener;
import org.apache.pekko.actor.ActorRef;

import java.util.Optional;

/**
 * A launchable intended to be used from a Configurator to notify an actor about new configuration.
 *
 * @param <T> The type representing the validated configuration.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class ActorConfigurator<T> implements Listener {
    /**
     * Public constructor.
     *
     * @param notifyTarget actor to notify of configuration update
     * @param configurationClass the class of the configuration
     */
    public ActorConfigurator(final ActorRef notifyTarget, final Class<? extends T> configurationClass) {
        _notifyTarget = notifyTarget;
        _configurationClass = configurationClass;
    }

    @Override
    public synchronized void offerConfiguration(final Configuration configuration) throws ConfigurationException {
        try {
            _offeredConfiguration = configuration.getAs(_configurationClass);
        // CHECKSTYLE.OFF: IllegalCatch - The getAs method only throws RuntimeExceptions.
        } catch (final RuntimeException e) {
        // CHECKSTYLE.ON: IllegalCatch
            throw new ConfigurationException("Could not load configuration", e);
        }
    }

    @Override
    public synchronized void applyConfiguration() {
        if (!_offeredConfiguration.isPresent()) {
            throw new IllegalStateException("No offered configuration to apply.");
        }
        _notifyTarget.tell(new ConfigurableActorProxy.ApplyConfiguration<>(_offeredConfiguration.get()), ActorRef.noSender());
        _offeredConfiguration = Optional.empty();
    }

    private Optional<T> _offeredConfiguration = Optional.empty();
    private final ActorRef _notifyTarget;
    private final Class<? extends T> _configurationClass;
}
