/**
 * Copyright 2016 Inscope Metrics, Inc
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
package com.arpnetworking.akka;

import akka.actor.UntypedActor;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;

/**
 * Actor that does not attempt to join a cluster.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class NonJoiningClusterJoiner extends UntypedActor {
    /**
     * {@inheritDoc}
     */
    @Override
    public void onReceive(final Object message) throws Exception {
        unhandled(message);
    }

    private NonJoiningClusterJoiner(final Builder builder) {
        LOGGER.info()
                .setMessage("NonJoiningClusterJoiner starting up")
                .log();
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(NonJoiningClusterJoiner.class);

    /**
     * Implementation of the {@link com.arpnetworking.commons.builder.Builder} pattern for a {@link NonJoiningClusterJoiner}.
     *
     * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
     */
    public static class Builder extends ActorBuilder<Builder, NonJoiningClusterJoiner> {
        /**
         * Public constructor.
         */
        public Builder() {
            super(NonJoiningClusterJoiner::new);
        }

        /**
         * {@inheritDoc}
         */
        @Override
        public Builder self() {
            return this;
        }

        private static final long serialVersionUID = 1L;
    }
}
