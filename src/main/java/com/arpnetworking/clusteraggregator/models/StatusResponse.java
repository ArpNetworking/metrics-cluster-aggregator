/**
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

import akka.actor.Address;
import akka.cluster.Member;
import com.arpnetworking.clusteraggregator.ClusterStatusCache;
import com.arpnetworking.commons.builder.OvalBuilder;
import com.arpnetworking.commons.jackson.databind.ObjectMapperFactory;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.common.base.Charsets;
import com.google.common.collect.Iterables;
import com.google.common.io.Resources;
import net.sf.oval.constraint.NotEmpty;
import net.sf.oval.constraint.NotNull;
import org.joda.time.Period;
import scala.collection.JavaConversions;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Response model for the status http endpoint.
 *
 * @author Brandon Arp (brandonarp at gmail dot com)
 */
public final class StatusResponse {
    public String getClusterLeader() {
        return _clusterLeader != null ? _clusterLeader.toString() : null;
    }

    public String getLocalAddress() {
        return _localAddress.toString();
    }

    public BookkeeperData getMetrics() {
        return _metrics;
    }

    @JsonProperty("isLeader")
    public boolean isLeader() {
        return _localAddress.equals(_clusterLeader);
    }

    @JsonSerialize(contentUsing = MemberSerializer.class)
    public Iterable<Member> getMembers() {
        return Iterables.unmodifiableIterable(_members);
    }

    public Map<Period, PeriodMetrics> getLocalMetrics() {
        return Collections.unmodifiableMap(_localMetrics);
    }

    public Optional<List<ShardAllocation>> getAllocations() {
        return _allocations.map(Collections::unmodifiableList);
    }

    public String getVersion() {
        return VERSION_INFO.getVersion();
    }

    public String getName() {
        return VERSION_INFO.getName();
    }

    public String getSha() {
        return VERSION_INFO.getSha();
    }

    private StatusResponse(final Builder builder) {
        if (builder._clusterState == null) {
            _clusterLeader = null;
            _members = Collections.emptyList();
        } else {
            _clusterLeader = builder._clusterState.getClusterState().getLeader();
            _members = builder._clusterState.getClusterState().getMembers();
        }

        _localAddress = builder._localAddress;
        _metrics = builder._bookkeeperData;
        _localMetrics = builder._localMetrics;
        _allocations = flatten(
                Optional.ofNullable(builder._clusterState)
                        .map(ClusterStatusCache.StatusResponse::getAllocations));
    }

    private <T> Optional<T> flatten(final Optional<Optional<T>> value) {
        if (value.isPresent()) {
            return value.get();
        }
        return Optional.empty();
    }

    private final Address _localAddress;
    private final Address _clusterLeader;
    private final BookkeeperData _metrics;
    private final Iterable<Member> _members;
    private final Map<Period, PeriodMetrics> _localMetrics;
    private final Optional<List<ShardAllocation>> _allocations;

    private static final VersionInfo VERSION_INFO;
    private static final Logger LOGGER = LoggerFactory.getLogger(StatusResponse.class);

    static {
        StatusResponse.VersionInfo versionInfo = null;
        try {
            versionInfo =
                    ObjectMapperFactory.getInstance().readValue(
                            Resources.toString(Resources.getResource("status.json"), Charsets.UTF_8),
                            StatusResponse.VersionInfo.class);
        } catch (final IOException e) {
            LOGGER.error()
                    .setMessage("Resource load failure")
                    .addData("resource", "status.json")
                    .setThrowable(e)
                    .log();
            versionInfo = new StatusResponse.VersionInfo.Builder()
                    .setVersion("UNKNOWN")
                    .setName("UNKNOWN")
                    .setSha("")
                    .build();
        }
        VERSION_INFO = versionInfo;
    }

    /**
     * Builder for a {@link StatusResponse}.
     */
    public static class Builder extends OvalBuilder<StatusResponse> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(StatusResponse::new);
        }

        /**
         * Sets the cluster state. Optional.
         *
         * @param value The cluster state.
         * @return This builder.
         */
        public Builder setClusterState(final ClusterStatusCache.StatusResponse value) {
            _clusterState = value;
            return this;
        }

        /**
         * Sets the bookkeeper data. Optional.
         *
         * @param value The bookkeeper data.
         * @return This builder.
         */
        public Builder setClusterMetrics(final BookkeeperData value) {
            _bookkeeperData = value;
            return this;
        }

        /**
         * Sets the local address of this cluster node. Required. Cannot be null.
         *
         * @param value The address of the local node.
         * @return This builder.
         */
        public Builder setLocalAddress(final Address value) {
            _localAddress = value;
            return this;
        }

        /**
         * Sets the local metrics for this cluster node.
         *
         * @param value The local metrics.
         * @return This builder.
         */
        public Builder setLocalMetrics(final Map<Period, PeriodMetrics> value) {
            _localMetrics = value;
            return this;
        }

        private ClusterStatusCache.StatusResponse _clusterState;
        private BookkeeperData _bookkeeperData;
        private Address _localAddress;
        private Map<Period, PeriodMetrics> _localMetrics;
    }

    /**
     * Represents the model for the version of the service currently running.
     *
     * @author Brandon Arp (brandon dot arp at smartsheet dot com)
     */
    private static final class VersionInfo {
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

    private static final class MemberSerializer extends JsonSerializer<Member> {
        /**
         * {@inheritDoc}
         */
        @Override
        public void serialize(
                final Member value,
                final JsonGenerator gen,
                final SerializerProvider serializers)
                throws IOException {
            gen.writeStartObject();
            gen.writeStringField("address", value.address().toString());
            gen.writeObjectField("roles", JavaConversions.setAsJavaSet(value.roles()));
            gen.writeNumberField("upNumber", value.upNumber());
            gen.writeStringField("status", value.status().toString());
            gen.writeNumberField("uniqueAddress", value.uniqueAddress().longUid());
            gen.writeEndObject();
        }
    }
}
