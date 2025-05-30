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
 * Package containing trigger implementations for configuration changes.
 * <p>
 * This package provides various trigger mechanisms that detect changes in resources
 * such as files, directories, and URIs. These triggers are used primarily with
 * dynamic configuration systems to reload or update configurations when the
 * underlying resources change.
 * </p>
 *
 * @author Inscope Metrics
 */
@ParametersAreNonnullByDefault
@ReturnValuesAreNonnullByDefault
package com.arpnetworking.configuration.triggers;

import com.arpnetworking.commons.javax.annotation.ReturnValuesAreNonnullByDefault;

import javax.annotation.ParametersAreNonnullByDefault;
