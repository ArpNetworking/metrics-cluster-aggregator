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
 * Package containing client connection and communication components.
 * <p>
 * This package provides classes for managing client connections to the cluster
 * aggregator service, including TCP server components, client connection handlers,
 * and HTTP source actors that receive and process metrics data from clients.
 * </p>
 *
 * @author Inscope Metrics
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package com.arpnetworking.clusteraggregator.client;

import com.arpnetworking.commons.javax.annotation.ReturnValuesAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;
