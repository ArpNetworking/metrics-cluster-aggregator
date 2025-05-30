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
 * Package containing utilities for data partitioning.
 * <p>
 * This package provides interfaces and classes for managing partitions of data
 * or workloads. It includes functionality for mapping keys to partitions and
 * creating partition sets. Partitioning is commonly used in distributed systems
 * to distribute data or processing across multiple nodes for scalability and
 * load balancing.
 * </p>
 *
 * @author Inscope Metrics
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package com.arpnetworking.utility.partitioning;

import com.arpnetworking.commons.javax.annotation.ReturnValuesAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;
