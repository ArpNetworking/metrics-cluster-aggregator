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
package com.arpnetworking.tsdcore.model;

import akka.util.ByteString;
import akka.util.ByteStringBuilder;
import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessageV3;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Tests for the AggregationMessage class.
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public class AggregationMessageTest {

    @Test
    public void testHostIdentification() {
        final GeneratedMessageV3 protobufMessage = Messages.HostIdentification.getDefaultInstance();
        final AggregationMessage message = AggregationMessage.create(protobufMessage);
        Assert.assertNotNull(message);
        Assert.assertSame(protobufMessage, message.getMessage());

        final ByteString byteString = message.serialize();
        final byte[] messageBuffer = byteString.toArray();
        final byte[] protobufBuffer = protobufMessage.toByteArray();

        // Assert length
        Assert.assertEquals(protobufBuffer.length + 5, messageBuffer.length);
        Assert.assertEquals(protobufBuffer.length + 5, byteString.iterator().getInt(ByteOrder.BIG_ENDIAN));
        Assert.assertEquals(protobufBuffer.length + 5, message.getLength());

        // Assert payload type
        Assert.assertEquals(1, messageBuffer[4]);

        // Assert the payload was not corrupted
        for (int i = 0; i < protobufBuffer.length; ++i) {
            Assert.assertEquals(protobufBuffer[i], messageBuffer[i + 5]);
        }

        // Deserialize the message
        final Optional<AggregationMessage> deserializedProtobufMessage = AggregationMessage.deserialize(byteString);
        Assert.assertTrue(deserializedProtobufMessage.isPresent());
        Assert.assertEquals(protobufMessage, deserializedProtobufMessage.get().getMessage());
        Assert.assertEquals(message.getLength(), deserializedProtobufMessage.get().getLength());
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSerializedUnsupportedMessage() {
        final GeneratedMessageV3 message = Messages.SparseHistogramEntry.newBuilder()
                .build();
        AggregationMessage.create(message).serialize();
    }

    @Test
    public void testDeserializeWrongLength() {
        ByteString buffer;
        Optional<AggregationMessage> message;

        // Too little data
        buffer = new ByteStringBuilder().putInt(Integer.SIZE / 8 - 1, ByteOrder.BIG_ENDIAN).result();
        message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);

        // Too much data
        buffer = new ByteStringBuilder().putInt(Integer.SIZE / 8 + 1, ByteOrder.BIG_ENDIAN).result();
        message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);

        // No data
        buffer = new ByteStringBuilder().putInt(0, ByteOrder.BIG_ENDIAN).result();
        message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);

        // Negative data
        buffer = ByteString.fromInts(-1);
        message = AggregationMessage.deserialize(buffer);
    }

    @Test
    public void testDeserializeUnsupportedType() {
        ByteString buffer;
        Optional<AggregationMessage> message;

        // Type: 0
        buffer = new ByteStringBuilder().putInt(Integer.SIZE / 8 + 1, ByteOrder.BIG_ENDIAN).putByte((byte) 0).result();
        message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);

        // Type: 3
        buffer = new ByteStringBuilder().putInt(Integer.SIZE / 8 + 1, ByteOrder.BIG_ENDIAN).putByte((byte) 9).result();
        message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);

        // Type: -1
        buffer = new ByteStringBuilder().putInt(Integer.SIZE / 8 + 1, ByteOrder.BIG_ENDIAN).putByte((byte) -1).result();
        message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);
    }

    @Test
    public void testDeserializeInvalidPayload() {
        // Just one byte
        final ByteString buffer = new ByteStringBuilder()
                .putInt(Integer.SIZE / 8 + 2, ByteOrder.BIG_ENDIAN)
                .putByte((byte) 1)
                .putByte((byte) 0)
                .result();
        final Optional<AggregationMessage> message = AggregationMessage.deserialize(buffer);
        Assert.assertEquals(Optional.empty(), message);
    }
}
