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
 * Package containing sink implementations for publishing metrics data.
 * <p>
 * This package provides various implementations of the Sink interface, which is
 * responsible for publishing aggregated metrics data to different destinations.
 * It includes sinks for popular time series databases and monitoring systems
 * (such as InfluxDB, KairosDB, Graphite/Carbon, DataDog, SignalFx), as well as
 * utility sinks for filtering, transforming, and routing metrics data.
 * </p>
 *
 * @author Inscope Metrics
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.commons.javax.annotation.ReturnValuesAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;
