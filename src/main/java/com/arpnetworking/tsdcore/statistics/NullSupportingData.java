/*
 * Copyright 2024 InscopeMetrics
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
package com.arpnetworking.tsdcore.statistics;


import java.io.Serializable;

/**
 * This class represents no supporting data for a statistic. It is used to
 * represent the case where a statistic does not have any supporting data.
 * This class cannot be instantiated.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class NullSupportingData implements Serializable {
    private NullSupportingData() {}
    private static final long serialVersionUID = 1L;
}
