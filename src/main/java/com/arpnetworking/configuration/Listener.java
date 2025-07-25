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
package com.arpnetworking.configuration;

/**
 * Interface for consumers registered for configuration events.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public interface Listener {

    /**
     * Invoked before new configuration is applied.  Any registered listener
     * may reject the configuration by throwing an {@link ConfigurationException}. Any
     * listener rejecting the configuration rejects the entire configuration
     * and the offering instance will log the {@link ConfigurationException} with an
     * error. Once any listener rejects the {@link Configuration} other
     * listeners may not be offered that instance.
     *
     * @param configuration The new {@link Configuration} to be validated.
     * @throws ConfigurationException Thrown if the {@link Configuration} should be
     * rejected due to validation errors or other configuration issues.
     */
    void offerConfiguration(Configuration configuration) throws ConfigurationException;

    /**
     * Invoked to apply the most recently offered configuration. Any
     * {@link RuntimeException} thrown is logged and ignored. All
     * validation must be performed during offer.
     */
    void applyConfiguration();
}
