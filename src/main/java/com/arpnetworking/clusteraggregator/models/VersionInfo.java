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
package com.arpnetworking.clusteraggregator.models;

import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;

import java.io.IOException;

/**
 * Represents the model for the version of the service currently running.
 *
 * @author Brandon Arp (brandon dot arp at smartsheet dot com)
 */
public final class VersionInfo {

    public static VersionInfo getInstance() {
        return VERSION_INFO;
    }

    public String getName() {
        return _name;
    }

    public String getVersion() {
        return _version;
    }

    public String getSha() {
        return _sha;
    }

    private VersionInfo(final Builder builder) {
        _name = builder._name;
        _version = builder._version;
        _sha = builder._sha;
    }

    private String _name;
    private String _version;
    private String _sha;

    private static final VersionInfo VERSION_INFO;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatusResponse.class);

    static {
        VersionInfo versionInfo;
        try {
            versionInfo =
                    ObjectMapperFactory.getInstance().readValue(
                            Resources.toString(Resources.getResource("version.json"), Charsets.UTF_8),
                            VersionInfo.class);
        } catch (final IOException e) {
            LOGGER.error()
                    .setMessage("Resource load failure")
                    .addData("resource", "version.json")
                    .setThrowable(e)
                    .log();
            versionInfo = new VersionInfo.Builder()
                    .setVersion("UNKNOWN")
                    .setName("UNKNOWN")
                    .setSha("UNKNOWN")
                    .build();
        }
        VERSION_INFO = versionInfo;
    }

    /**
     * Builder for a {@link VersionInfo}.
     */
    private static final class Builder extends OvalBuilder<VersionInfo> {
        /**
         * Public constructor.
         */
        private Builder() {
            super(VersionInfo::new);
        }

        /**
         * Sets the name of the application. Required. Cannot be null. Cannot be empty.
         *
         * @param value The name of the application.
         * @return This builder.
         */
        public Builder setName(final String value) {
            _name = value;
            return this;
        }

        /**
         * Sets the name or tag of the version. Required. Cannot be null. Cannot be empty.
         *
         * @param value The name of the version.
         * @return This builder.
         */
        public Builder setVersion(final String value) {
            _version = value;
            return this;
        }

        /**
         * Sets the SHA. Required. Cannot be null. Cannot be empty.
         *
         * @param value The SHA.
         * @return This builder.
         */
        public Builder setSha(final String value) {
            _sha = value;
            return this;
        }

        @NotNull
        @NotEmpty
        private String _name;
        @NotNull
        @NotEmpty
        private String _version;
        @NotNull
        @NotEmpty
        private String _sha;
    }
}
