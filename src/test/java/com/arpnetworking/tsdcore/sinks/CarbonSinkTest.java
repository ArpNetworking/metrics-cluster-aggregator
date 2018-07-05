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

import akka.util.ByteString;
import com.arpnetworking.test.TestBeanFactory;
import com.arpnetworking.tsdcore.model.AggregatedData;
import com.arpnetworking.tsdcore.model.PeriodicData;
import com.arpnetworking.utility.BaseActorTest;
import com.google.common.base.Charsets;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.joda.time.Period;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.util.function.Consumer;

/**
 * Tests for the <code>CarbonSink</code> class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class CarbonSinkTest extends BaseActorTest {

    @Before
    public void before() {
        _carbonSinkBuilder = new CarbonSink.Builder()
                .setName("carbon_sink_test")
                .setServerAddress("my-carbon-server.example.com")
                .setServerPort(9999)
                .setActorSystem(getSystem());
    }

    @Test
    public void testSerialize() {
        final ArgumentCaptor<ByteString> recorded = ArgumentCaptor.forClass(ByteString.class);
        @SuppressWarnings("unchecked")
        final Consumer<ByteString> recorder = (Consumer<ByteString>) Mockito.mock(Consumer.class);
        final CarbonSink carbonSink = new CarbonTestSink(recorder, _carbonSinkBuilder);
        final AggregatedData datum = TestBeanFactory.createAggregatedData();
        final PeriodicData periodicData = new PeriodicData.Builder()
                .setData(ImmutableList.of(datum))
                .setDimensions(ImmutableMap.of("host", "MyHost"))
                .setPeriod(Period.minutes(1))
                .setStart(DateTime.now())
                .build();
        carbonSink.recordAggregateData(periodicData);
        Mockito.verify(recorder, Mockito.timeout(5000)).accept(Mockito.any());
        Mockito.verify(recorder, Mockito.timeout(5000)).accept(recorded.capture());
        final ByteString buffer = recorded.getValue();
        Assert.assertNotNull(buffer);
        String bufferString = buffer.decodeString(Charsets.UTF_8);
        Assert.assertTrue("Buffer=" + bufferString, bufferString.endsWith("\n"));
        bufferString = bufferString.substring(0, bufferString.length() - 1);
        final String[] keyValueParts = bufferString.split(" ");
        Assert.assertEquals("Buffer=" + bufferString, 3, keyValueParts.length);
        Assert.assertEquals("Buffer=" + bufferString, String.format("%f", datum.getValue().getValue()), keyValueParts[1]);
        Assert.assertEquals("Buffer=" + bufferString, String.valueOf(periodicData.getStart().getMillis() / 1000), keyValueParts[2]);
        final String[] keyParts = keyValueParts[0].split("\\.");
        Assert.assertEquals("Buffer=" + bufferString, 6, keyParts.length);
        Assert.assertEquals("Buffer=" + bufferString, datum.getFQDSN().getCluster(), keyParts[0]);
        Assert.assertEquals("Buffer=" + bufferString, periodicData.getDimensions().get("host"), keyParts[1]);
        Assert.assertEquals("Buffer=" + bufferString, datum.getFQDSN().getService(), keyParts[2]);
        Assert.assertEquals("Buffer=" + bufferString, datum.getFQDSN().getMetric(), keyParts[3]);
        Assert.assertEquals("Buffer=" + bufferString, periodicData.getPeriod().toString(), keyParts[4]);
        Assert.assertEquals("Buffer=" + bufferString, datum.getFQDSN().getStatistic().getName(), keyParts[5]);
    }

    private CarbonSink.Builder _carbonSinkBuilder;

    private static final class CarbonTestSink extends CarbonSink {
        private CarbonTestSink(final Consumer<ByteString> bufferConsumer, final Builder builder) {
            super(builder);
            _bufferConsumer = bufferConsumer;
        }

        @Override
        protected ByteString serializeData(final PeriodicData data) {
            final ByteString serialized = super.serializeData(data);
            _bufferConsumer.accept(serialized);
            return serialized;
        }

        private final Consumer<ByteString> _bufferConsumer;
    }
}
