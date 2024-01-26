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

import com.arpnetworking.metrics.aggregation.protocol.Messages;
import com.google.protobuf.GeneratedMessageV3;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.codec.binary.Hex;
import org.apache.pekko.util.ByteIterator;
import org.apache.pekko.util.ByteString;
import org.apache.pekko.util.ByteStringBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteOrder;
import java.util.Optional;

/**
 * Class for building messages from the raw, on-the-wire bytes in the TCP stream.
 *
 * @author Brandon Arp (brandon dot arp at inscopemetrics dot com)
 */
public final class AggregationMessage {

    /**
     * Static factory.
     *
     * @param message The message.
     * @return New {@link AggregationMessage} instance.
     */
    public static AggregationMessage create(final GeneratedMessageV3 message) {
        return new AggregationMessage(message);
    }

    /**
     * Deserialize message from {@link ByteString}.
     *
     * TODO(vkoskela): The header and message need to be versioned [MAI-133].
     *
     * @param data The {@link ByteString} containing the serialized message.
     * @return The deserialized {@link AggregationMessage} or absent if
     * the {@link ByteString} could not be deserialized.
     */
    public static Optional<AggregationMessage> deserialize(final ByteString data) {
        int position = 0;
        // Make sure we have enough data to get the size
        if (data.length() < HEADER_SIZE_IN_BYTES) {
            return Optional.empty();
        }

        // Deserialize and validate buffer length
        final ByteIterator reader = data.iterator();
        final int length = reader.getInt(ByteOrder.BIG_ENDIAN);
        position += INTEGER_SIZE_IN_BYTES;
        if (data.length() < length) {
            if (LOGGER.isTraceEnabled()) {
                LOGGER.trace(String.format("we only have %d of %d bytes.", data.length(), length));
            }
            return Optional.empty();
        }

        // Deserialize message type
        final byte type = reader.getByte();
        position += BYTE_SIZE_IN_BYTES;

        final byte subType;
        if (typeHasSubtype(type)) {
            subType = reader.getByte();
            position += BYTE_SIZE_IN_BYTES;
        } else {
            subType = 0x00;
        }

        // Obtain the serialized payload
        final byte[] payloadBytes = new byte [length - position];
        reader.getBytes(payloadBytes);

        // Deserialize the message based on the type
        try {
            switch (type) {
                case 0x01:
                    return Optional.of(new AggregationMessage(Messages.HostIdentification.parseFrom(payloadBytes), length));
                case 0x03:
                    return Optional.of(new AggregationMessage(Messages.HeartbeatRecord.parseFrom(payloadBytes), length));
                case 0x04:
                    return Optional.of(new AggregationMessage(Messages.StatisticSetRecord.parseFrom(payloadBytes), length));
                case 0x05:
                    // 0x05 is the message type for all supporting data
                    switch (subType) {
                        case 0x01:
                            return Optional.of(new AggregationMessage(Messages.SamplesSupportingData.parseFrom(payloadBytes), length));
                        case 0x02:
                            return Optional.of(new AggregationMessage(
                                    Messages.SparseHistogramSupportingData.parseFrom(payloadBytes),
                                    length));
                        default:
                            LOGGER.warn(
                                    String.format("Invalid protocol buffer, unknown subtype; type=%s, subtype=%s, bytes=%s",
                                            type,
                                            subType,
                                            Hex.encodeHexString(payloadBytes)));
                            return Optional.empty();
                    }
                default:
                    LOGGER.warn(String.format("Unsupported message type; type=%s", type));
                    return Optional.empty();
            }
        } catch (final InvalidProtocolBufferException e) {
            LOGGER.warn(
                    String.format("Invalid protocol buffer; type=%s bytes=%s", type, Hex.encodeHexString(payloadBytes)), e);
            return Optional.empty();
        }
    }

    private static boolean typeHasSubtype(final byte type) {
        return type == 0x05;
    }

    /**
     * Serialize the message into a {@link ByteString}.
     *
     * @return {@link ByteString} containing serialized message.
     */
    public ByteString serialize() {
        return AggregationMessage.serialize(_message);
    }

    private static ByteString serialize(final GeneratedMessageV3 message) {
        final ByteStringBuilder b = ByteString.createBuilder();
        if (message instanceof Messages.HostIdentification) {
            b.putByte((byte) 0x01);
        } else if (message instanceof Messages.HeartbeatRecord) {
            b.putByte((byte) 0x03);
        } else if (message instanceof Messages.StatisticSetRecord) {
            b.putByte((byte) 0x04);
        } else if (message instanceof Messages.SamplesSupportingData) {
            b.putByte((byte) 0x05);
            b.putByte((byte) 0x01);
        } else if (message instanceof Messages.SparseHistogramSupportingData) {
            b.putByte((byte) 0x05);
            b.putByte((byte) 0x02);
        } else {
            throw new IllegalArgumentException(String.format("Unsupported message; message=%s", message));
        }
        try {
            message.writeTo(b.asOutputStream());
        } catch (final IOException e) {
            throw new RuntimeException(e);
        }
        final ByteStringBuilder sizePrefix = ByteString.createBuilder();
        sizePrefix.putInt(b.length() + INTEGER_SIZE_IN_BYTES, ByteOrder.BIG_ENDIAN);
        return sizePrefix.result().concat(b.result());
    }

    public GeneratedMessageV3 getMessage() {
        return _message;
    }

    public int getLength() {
        return _length;
    }

    private AggregationMessage(final GeneratedMessageV3 message) {
        this(message, serialize(message).length());
    }

    private AggregationMessage(final GeneratedMessageV3 message, final int length) {
        _length = length;
        _message = message;
    }

    private final GeneratedMessageV3 _message;
    private final int _length;

    private static final int BYTE_SIZE_IN_BYTES = 1;
    private static final int INTEGER_SIZE_IN_BYTES = Integer.SIZE / 8;
    private static final int HEADER_SIZE_IN_BYTES = INTEGER_SIZE_IN_BYTES + 1;
    private static final Logger LOGGER = LoggerFactory.getLogger(AggregationMessage.class);
}
