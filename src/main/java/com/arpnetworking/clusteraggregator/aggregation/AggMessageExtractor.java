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
package com.arpnetworking.clusteraggregator.aggregation;

import akka.cluster.sharding.ShardRegion;
import com.arpnetworking.clusteraggregator.models.CombinedMetricData;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.arpnetworking.steno.Logger;
import com.arpnetworking.steno.LoggerFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.google.common.collect.Maps;

import java.util.Comparator;
import java.util.Map;
import java.util.TreeMap;

/**
 * Handles extracting the sharding information from an aggregation message.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class AggMessageExtractor implements ShardRegion.MessageExtractor {
    /**
     * {@inheritDoc}
     *
     * @param message The message instance.
     */
    @SuppressWarnings("deprecation")
    @Override
    public String entityId(final Object message) {
        if (message instanceof AggregatedData) {
            final AggregatedData aggregationMessage = (AggregatedData) message;
            final StringBuilder builder = new StringBuilder();
            builder.append(aggregationMessage.getFQDSN().getCluster())
                    .append(aggregationMessage.getFQDSN().getService())
                    .append(aggregationMessage.getFQDSN().getMetric())
                    .append(aggregationMessage.getPeriod())
                    .append(aggregationMessage.getFQDSN().getStatistic());
            return builder.toString();
        } else if (message instanceof Messages.StatisticSetRecord) {
            final Messages.StatisticSetRecord metricData = (Messages.StatisticSetRecord) message;
            final StringBuilder builder = new StringBuilder();
            builder.append(metricData.getCluster())
                    .append("||")
                    .append(metricData.getService())
                    .append("||")
                    .append(metricData.getMetric())
                    .append("||")
                    .append(metricData.getPeriod());

            final TreeMap<String, String> sortedDimensionsMap = dimensionsToMap(metricData);
            for (final Map.Entry<String, String> dimensionEntry : sortedDimensionsMap.entrySet()) {
                final String k = dimensionEntry.getKey();
                if (!(k.equals(CombinedMetricData.CLUSTER_KEY)
                        || k.equals(CombinedMetricData.HOST_KEY)
                        || k.equals(CombinedMetricData.SERVICE_KEY))) {
                    builder
                            .append("||")
                            .append(dimensionEntry.getKey())
                            .append("=")
                            .append(dimensionEntry.getValue());
                }
            }
            return builder.toString();
        }
        throw new IllegalArgumentException("Unknown message type " + message);
    }

    /**
     * Sort and de-dupe the dimensions in a consistent manner. Will log an error if any duplicate keys are found and select the
     * lexicographically-smaller value.
     *
     * @param metricData The <code>StatisticSetRecord</code> from which to pull dimensions.
     * @return A sorted, de-duped TreeMap of the dimensions.
     */
    private TreeMap<String, String> dimensionsToMap(final Messages.StatisticSetRecord metricData) {
        final TreeMap<String, String> sortedDimensionsMap = Maps.newTreeMap(Comparator.<String>naturalOrder());

        sortedDimensionsMap.putAll(metricData.getDimensionsMap());

        return sortedDimensionsMap;
    }

    /**
     * {@inheritDoc}
     *
     * @param message The message instance.
     */
    @Override
    public Object entityMessage(final Object message) {
        return message;
    }

    /**
     * {@inheritDoc}
     *
     * @param message The message instance.
     */
    @Override
    public String shardId(final Object message) {
        return String.format("shard_%d", Math.abs(entityId(message).hashCode() % SHARD_COUNT));
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(AggMessageExtractor.class);
    private static final int SHARD_COUNT = 10000;
}
