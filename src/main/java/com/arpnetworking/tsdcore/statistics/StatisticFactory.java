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
package com.arpnetworking.tsdcore.statistics;

import com.arpnetworking.utility.InterfaceDatabase;
import com.arpnetworking.utility.ReflectionsDatabase;
import com.google.common.collect.Maps;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Creates statistics.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class StatisticFactory {

    /**
     * Get a statistic by name.
     *
     * @param name The name of the desired statistic.
     * @return A new <code>Statistic</code>.
     */
    public Statistic getStatistic(final String name) {
        final Optional<Statistic> statistic = tryGetStatistic(name);
        if (!statistic.isPresent()) {
            throw new IllegalArgumentException(String.format("Invalid statistic name; name=%s", name));
        }
        return statistic.get();
    }

    /**
     * Get a statistic by name.
     *
     * @param name The name of the desired statistic.
     * @return A new <code>Statistic</code>.
     */
    public Optional<Statistic> tryGetStatistic(final String name) {
        final Optional<Statistic> registeredStatistic =
                Optional.ofNullable(STATISTICS_BY_NAME_AND_ALIAS.get(name));
        if (!registeredStatistic.isPresent()) {
            final Optional<Statistic> statistic = getPercentileStatistic(name);
            if (statistic.isPresent()) {
                checkedPut(STATISTICS_BY_NAME_AND_ALIAS, statistic.get());
                return statistic;
            }
        }
        return registeredStatistic;
    }

    private static Optional<Statistic> getPercentileStatistic(final String name) {
        final Matcher matcher = PERCENTILE_STATISTIC_PATTERN.matcher(name);
        if (matcher.matches()) {
            try {
                final String percentileString = matcher.group("percentile").replace('p', '.');
                final double percentile = Double.parseDouble(percentileString);
                final Statistic statistic = new TPStatistic(percentile);
                return Optional.of(statistic);
            } catch (final NumberFormatException e) {
                LOGGER.error()
                        .setMessage("Invalid percentile statistic")
                        .addData("name", name)
                        .log();
            }
        }
        return Optional.empty();
    }
    
    private static void checkedPut(final ConcurrentMap<String, Statistic> map, final Statistic statistic) {
        checkedPut(map, statistic, statistic.getName());
        for (final String alias : statistic.getAliases()) {
            checkedPut(map, statistic, alias);
        }
    }

    private static void checkedPut(final ConcurrentMap<String, Statistic> map, final Statistic statistic, final String key) {
        final Statistic existingStatistic =  map.get(key);
        if (existingStatistic != null) {
            if (!existingStatistic.equals(statistic)) {
                LOGGER.error(String.format(
                        "Statistic already registered; key=%s, existing=%s, new=%s",
                        key,
                        existingStatistic,
                        statistic));
            }
            return;
        }
        map.put(key, statistic);
    }

    private static final Pattern PERCENTILE_STATISTIC_PATTERN = Pattern.compile("^[t]?p(?<percentile>[0-9]+(?:(\\.|p)[0-9]+)?)$");
    private static final ConcurrentMap<String, Statistic> STATISTICS_BY_NAME_AND_ALIAS;
    private static final InterfaceDatabase INTERFACE_DATABASE = ReflectionsDatabase.newInstance();
    private static final ImmutableList<String> PERCENTILE_STATISTICS_TO_PRELOAD = ImmutableList.of("p75", "p90", "p95", "p99", "p99.9");
    private static final Logger LOGGER = LoggerFactory.getLogger(StatisticFactory.class);

    static {
        // NOTE: Do not put log messages in static blocks since they can lock the logger thread!
        final ConcurrentMap<String, Statistic> statisticByNameAndAlias = Maps.newConcurrentMap();
        final Set<Class<? extends Statistic>> statisticClasses = INTERFACE_DATABASE.findClassesWithInterface(Statistic.class);
        for (final Class<? extends Statistic> statisticClass : statisticClasses) {
            if (!statisticClass.isInterface() && !Modifier.isAbstract(statisticClass.getModifiers()) 
                    && !TPStatistic.class.equals(statisticClass)) {
                try {
                    final Constructor<? extends Statistic> constructor = statisticClass.getDeclaredConstructor();
                    if (!constructor.isAccessible()) {
                        constructor.setAccessible(true);
                    }
                    checkedPut(statisticByNameAndAlias, constructor.newInstance());
                } catch (final InvocationTargetException | NoSuchMethodException
                        | InstantiationException | IllegalAccessException e) {
                    LOGGER.warn(String.format("Unable to load statistic; class=%s", statisticClass), e);
                }
            }
        }
        for (final String percentileStatisticName : PERCENTILE_STATISTICS_TO_PRELOAD) {
            final Optional<Statistic> statistic = getPercentileStatistic(percentileStatisticName);
            if (statistic.isPresent()) {
                checkedPut(statisticByNameAndAlias, statistic.get());
            } else {
                LOGGER.warn()
                        .setMessage("Unable to load statistic")
                        .addData("name", percentileStatisticName)
                        .log();
            }
        }
        STATISTICS_BY_NAME_AND_ALIAS = statisticByNameAndAlias;
    }
}
