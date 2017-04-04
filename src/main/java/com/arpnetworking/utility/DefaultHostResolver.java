/**
 * Copyright 2014 Brandon Arp
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

import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * Resolves a host name using the built-in java functions.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class DefaultHostResolver implements HostResolver {

    @Override
    public String getLocalHostName() throws UnknownHostException {
        return InetAddress.getLocalHost().getCanonicalHostName();
    }
}
