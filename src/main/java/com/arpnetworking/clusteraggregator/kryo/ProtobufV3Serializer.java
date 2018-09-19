/*
 * Copyright 2018 Dropbox
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
package com.arpnetworking.clusteraggregator.kryo;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.google.protobuf.GeneratedMessageV3;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Protocol Buffers V3 serializer for Kryo based heavily on
 * {@code de.javakaffee.kryoserializers.protobuf.ProtobufSerializer} from
 * {@code de.javakaffee:kryo-serializers}.
 *
 * @param <T> the type to serialize and deserialize
 *
 * @author Ville Koskela (ville dot koskela at inscopemetrics dot com)
 */
public final class ProtobufV3Serializer<T extends GeneratedMessageV3> extends Serializer<T> {

    private Method _parseFromMethod = null;

    @Override
    public void write(final Kryo kryo, final Output output, final T protobufMessage) {
        // If our protobuf is null
        if (protobufMessage == null) {
            // Write our special null value
            output.writeByte(Kryo.NULL);
            output.flush();

            // and we're done
            return;
        }

        // Otherwise serialize protobuf to a byteArray
        final byte[] bytes = protobufMessage.toByteArray();

        // Write the length of our byte array
        output.writeInt(bytes.length + 1, true);

        // Write the byte array out
        output.writeBytes(bytes);
        output.flush();
    }

    @SuppressWarnings("unchecked")
    @Override
    public T read(final Kryo kryo, final Input input, final Class<T> type) {
        // Read the length of our byte array
        final int length = input.readInt(true);

        // If the length is equal to our special null value
        if (length == Kryo.NULL) {
            // Just return null
            return null;
        }
        // Otherwise read the byte array length
        final byte[] bytes = input.readBytes(length - 1);
        try {
            // Deserialize protobuf
            return (T) (getParseFromMethod(type).invoke(type, bytes));
        } catch (final NoSuchMethodException | InvocationTargetException | IllegalAccessException e) {
            throw new RuntimeException("Unable to deserialize protobuf " + e.getMessage(), e);
        }
    }

    @Override
    public boolean getAcceptsNull() {
        return true;
    }

    private Method getParseFromMethod(final Class<T> type) throws NoSuchMethodException {
        if (_parseFromMethod == null) {
            _parseFromMethod = type.getMethod("parseFrom", byte[].class);
        }
        return _parseFromMethod;
    }
}
