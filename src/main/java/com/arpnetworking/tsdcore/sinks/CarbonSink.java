/*
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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.google.common.base.Charsets;
import org.apache.pekko.util.ByteString;
import org.apache.pekko.util.ByteStringBuilder;

/**
 * Publisher to send data to a Carbon server.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class CarbonSink extends TcpSink {


    @Override
    protected ByteString serializeData(final PeriodicData periodicData) {
        final ByteStringBuilder builder = ByteString.createBuilder();
        for (final AggregatedData datum : periodicData.getData()) {
            builder.putBytes(
                    String.format(
                            "%s.%s.%s.%s.%s.%s %f %d%n",
                            datum.getFQDSN().getCluster(),
                            periodicData.getDimensions().get("host"),
                            datum.getFQDSN().getService(),
                            datum.getFQDSN().getMetric(),
                            periodicData.getPeriod().toString(),
                            datum.getFQDSN().getStatistic().getName(),
                            datum.getValue().getValue(),
                            periodicData.getStart().toEpochSecond())
                    .getBytes(Charsets.UTF_8));
        }
        return builder.result();
    }

    /* package private */ CarbonSink(final Builder builder) {
        super(builder);
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(CarbonSink.class);

    /**
     * Implementation of builder pattern for {@link CarbonSink}.
     *
     * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
     */
    public static final class Builder extends TcpSink.Builder<Builder, CarbonSink> {

        /**
         * Public constructor.
         */
        public Builder() {
            super(CarbonSink::new);
            setServerPort(2003);
        }

        @Override
        protected Builder self() {
            return this;
        }
    }
}
