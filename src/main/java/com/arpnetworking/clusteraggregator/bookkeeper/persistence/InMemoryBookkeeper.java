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

package com.arpnetworking.clusteraggregator.bookkeeper.persistence;

import com.arpnetworking.clusteraggregator.models.BookkeeperData;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.google.common.collect.Sets;

import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;

/**
 * Keeps the bookkeeper data in memory.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class InMemoryBookkeeper implements BookkeeperPersistence {
    @Override
    public void insertMetric(final AggregatedData data) {
        _clusters.add(clusterName(data));
        _services.add(serviceName(data));
        _metrics.add(metricName(data));
        _statistics.add(statisticName(data));
    }

    @Override
    public CompletionStage<BookkeeperData> getBookkeeperData() {
        return CompletableFuture.completedFuture(
                new BookkeeperData.Builder()
                        .setClusters((long) _clusters.size())
                        .setMetrics((long) _metrics.size())
                        .setServices((long) _services.size())
                        .setStatistics((long) _statistics.size())
                        .build());
    }

    @SuppressWarnings("deprecation")
    private String statisticName(final AggregatedData data) {
        return new StringBuilder()
                .append(data.getFQDSN().getCluster())
                .append("/")
                .append(data.getFQDSN().getService())
                .append("/")
                .append(data.getFQDSN().getMetric())
                .append("/")
                .append(data.getPeriod())
                .append("/")
                .append(data.getFQDSN().getStatistic().getName())
                .toString();
    }

    private String metricName(final AggregatedData data) {
        return data.getFQDSN().getService() + "/" + data.getFQDSN().getMetric();
    }

    private String serviceName(final AggregatedData data) {
        return data.getFQDSN().getService();
    }

    private String clusterName(final AggregatedData data) {
        return data.getFQDSN().getCluster();
    }

    private final Set<String> _clusters = Sets.newHashSet();
    private final Set<String> _services = Sets.newHashSet();
    private final Set<String> _metrics = Sets.newHashSet();
    private final Set<String> _statistics = Sets.newHashSet();
}
