/*
 * Copyright 2018 Inscope Metrics, Inc
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
package com.arpnetworking.clusteraggregator.models;

import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.Multimap;
import org.apache.pekko.util.ByteString;

/**
 * Represents a parsable HTTP request.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class HttpRequest {

    public String getPath() {
        return _path;
    }

    public Multimap<String, String> getHeaders() {
        return _headers;
    }

    public ByteString getBody() {
        return _body;
    }

    /**
     * Public constructor.
     *
     * @param path The path.
     * @param headers The headers.
     * @param body The body of the request.
     */
    public HttpRequest(
            final String path,
            final ImmutableMultimap<String, String> headers,
            final ByteString body) {
        _path = path;
        _headers = headers;
        _body = body;
    }

    private final String _path;
    private final ImmutableMultimap<String, String> _headers;
    private final ByteString _body;
}

