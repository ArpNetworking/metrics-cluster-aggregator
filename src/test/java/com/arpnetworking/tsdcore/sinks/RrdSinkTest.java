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
package com.arpnetworking.tsdcore.sinks;

import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.tsdcore.statistics.Statistic;
import com.arpnetworking.tsdcore.statistics.StatisticFactory;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.io.Files;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.List;

/**
 * Tests for the {@link RrdSink} class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class RrdSinkTest {

    @Before
    public void before() throws IOException {
        _path = Files.createTempDir();
        final File rrdToolFile = getRrdToolFile();
        final File outFile = getOutFile();
        Files.asCharSink(rrdToolFile, Charsets.UTF_8).write("#!/bin/bash\necho \"$@\" >> " + outFile.getAbsolutePath());
        rrdToolFile.setExecutable(true);
        _rrdSinkBuilder = new RrdSink.Builder()
                .setName("rrd_sink_test")
                .setPath(_path.getAbsolutePath())
                .setRrdTool(rrdToolFile.getAbsolutePath());
    }

    @Test
    public void testClose() {
        final Sink rrdSink = _rrdSinkBuilder.build();
        rrdSink.close();
        Assert.assertFalse(getOutFile().exists());
    }

    @Test
    public void testRecordProcessedAggregateData() throws IOException {
        final Sink rrdSink = _rrdSinkBuilder.build();
        final PeriodicData periodicData = TestBeanFactory.createPeriodicData();
        rrdSink.recordAggregateData(periodicData);

        final List<String> outLines = Files.readLines(getOutFile(), Charsets.UTF_8);
        Assert.assertEquals(2, outLines.size());
        final String[] createLine = outLines.get(0).split(" ");
        final String[] updateLine = outLines.get(1).split(" ");
        Assert.assertEquals(8, createLine.length);
        Assert.assertEquals("create", createLine[0]);
        Assert.assertEquals(3, updateLine.length);
        Assert.assertEquals("update", updateLine[0]);
    }

    @Test
    public void testMultipleRecordProcessedAggregateData() throws IOException {
        final Sink rrdSink = _rrdSinkBuilder.build();
        final PeriodicData periodicDataA = TestBeanFactory.createPeriodicDataBuilder()
                .setData(ImmutableList.of(
                        TestBeanFactory.createAggregatedDataBuilder()
                                .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                        .setMetric("metric")
                                        .setStatistic(MEAN_STATISTIC)
                                        .build())
                                .setHost("localhost")
                                .setPeriod(Period.minutes(5))
                                .build()))
                .setDimensions(ImmutableMap.of("host", "MyHost"))
                .build();
        rrdSink.recordAggregateData(periodicDataA);

        // Simulate rrd file creation
        Assert.assertTrue(getRrdFile(periodicDataA).createNewFile());

        final PeriodicData periodicDataB = TestBeanFactory.createPeriodicDataBuilder()
                .setData(ImmutableList.of(
                        TestBeanFactory.createAggregatedDataBuilder()
                                .setFQDSN(TestBeanFactory.createFQDSNBuilder()
                                        .setMetric("metric")
                                        .setStatistic(MEAN_STATISTIC)
                                        .build())
                                .setHost("localhost")
                                .setPeriod(Period.minutes(5))
                                .build()))
                .setDimensions(ImmutableMap.of("host", "MyHost"))
                .build();
        rrdSink.recordAggregateData(periodicDataB);

        final List<String> outLines = Files.readLines(getOutFile(), Charsets.UTF_8);
        Assert.assertEquals(3, outLines.size());
        final String[] createLine = outLines.get(0).split(" ");
        final String[] updateLineA = outLines.get(1).split(" ");
        final String[] updateLineB = outLines.get(2).split(" ");
        Assert.assertEquals(8, createLine.length);
        Assert.assertEquals("create", createLine[0]);
        Assert.assertEquals(3, updateLineA.length);
        Assert.assertEquals("update", updateLineA[0]);
        Assert.assertEquals(3, updateLineB.length);
        Assert.assertEquals("update", updateLineB[0]);
    }

    private File getRrdToolFile() {
        return new File(_path.getAbsolutePath() + File.separator + "rrdtool");
    }

    private File getOutFile() {
        return new File(_path.getAbsolutePath() + File.separator + "rrdtool.out");
    }

    private File getRrdFile(final PeriodicData periodicData) {
        final AggregatedData datum = periodicData.getData().get(0);
        return new File(_path.getAbsolutePath()
                + File.separator
                + (periodicData.getDimensions().get("host") + "."
                        + datum.getFQDSN().getMetric() + "."
                        + periodicData.getPeriod().toString()
                        + datum.getFQDSN().getStatistic().getName()
                        + ".rrd").replace("/", "-"));
    }

    private RrdSink.Builder _rrdSinkBuilder;
    private File _path;

    private static final StatisticFactory STATISTIC_FACTORY = new StatisticFactory();
    private static final Statistic MEAN_STATISTIC = STATISTIC_FACTORY.getStatistic("mean");
}
