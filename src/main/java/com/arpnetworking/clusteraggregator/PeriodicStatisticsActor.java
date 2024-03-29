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

package com.arpnetworking.clusteraggregator;

import com.arpnetworking.clusteraggregator.models.MetricsRequest;
import com.arpnetworking.clusteraggregator.models.PeriodMetrics;
import com.arpnetworking.metrics.MetricsFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.google.common.collect.Maps;
import org.apache.pekko.actor.AbstractActor;
import org.apache.pekko.actor.Props;

import java.time.Duration;
import java.util.Map;

/**
 * Actor that listens for metrics messages, updates internal state, and emits them.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public class PeriodicStatisticsActor extends AbstractActor {
    /**
     * Creates a {@link Props} for construction in Pekko.
     *
     * @param metricsFactory A {@link MetricsFactory} to use for metrics creation.
     * @return A new {@link Props}.
     */
    public static Props props(final MetricsFactory metricsFactory) {
        return Props.create(PeriodicStatisticsActor.class, metricsFactory);
    }

    /**
     * Public constructor.
     *
     * @param metricsFactory A {@link MetricsFactory} to use for metrics creation.
     */
    public PeriodicStatisticsActor(final MetricsFactory metricsFactory) {
        _metricsFactory = metricsFactory;
    }

    @SuppressWarnings("deprecation")
    @Override
    public Receive createReceive() {
        return receiveBuilder()
                .match(AggregatedData.class, report -> {
                    final Duration period = report.getPeriod();
                    PeriodMetrics metrics = _periodMetrics.get(period);
                    if (metrics == null) {
                        metrics = new PeriodMetrics(_metricsFactory);
                        _periodMetrics.put(period, metrics);
                    }

                    metrics.recordAggregation(report);
                })
                .match(MetricsRequest.class, message -> getSender().tell(_periodMetrics, getSelf()))
                .build();
    }

    private final Map<Duration, PeriodMetrics> _periodMetrics = Maps.newHashMap();
    private final MetricsFactory _metricsFactory;
}
