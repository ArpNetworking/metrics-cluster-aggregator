/*
 * Copyright 2020 Inscope Metrics
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

/**
 * Package containing data models for the Circonus API.
 * <p>
 * This package provides model classes that represent entities and responses from
 * the Circonus API. These models are used by the Circonus sink implementation to
 * interact with the Circonus platform, including managing brokers and check bundles.
 * The classes in this package map directly to the JSON structures used in the
 * Circonus API.
 * </p>
 *
 * @author Inscope Metrics
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package com.arpnetworking.tsdcore.sinks.circonus.api;

import com.arpnetworking.commons.javax.annotation.ReturnValuesAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;
