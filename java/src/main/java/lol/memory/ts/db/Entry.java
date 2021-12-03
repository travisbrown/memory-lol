package lol.memory.ts.db;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Optional;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class Entry {
    private static final Logger logger = LoggerFactory.getLogger(Entry.class);
    private final byte[] key;
    private final boolean checkPrevious;

    public final byte[] getKey() {
        return this.key;
    }

    public final boolean checkPrevious() {
        return this.checkPrevious;
    }

    public abstract Optional<byte[]> updateValue(Optional<byte[]> previousValue);

    Entry(byte[] key, boolean checkPrevious) {
        this.key = key;
        this.checkPrevious = checkPrevious;
    }

    Entry(byte[] key) {
        this(key, true);
    }

    static long[] bytesToLongs(byte[] bytes) {
        var count = bytes.length / 8;
        var buffer = ByteBuffer.wrap(bytes).asLongBuffer();
        var values = new long[count];
        buffer.get(values);
        return values;
    }

    static byte[] longsToBytes(List<Long> values) {
        byte[] result = new byte[values.size() * 8];
        for (int i = 0; i < values.size(); i += 1) {
            Entry.writeLong(result, i * 8, values.get(i));
        }
        return result;
    }

    static long readLong(final byte[] bytes, final int position) {
        long result = 0;
        for (int i = position; i < position + Long.BYTES; i += 1) {
            result <<= Byte.SIZE;
            result |= (bytes[i] & 0xFF);
        }
        return result;
    }

    static void writeLong(final byte[] bytes, final int position, final long value) {
        bytes[position] = (byte) (value >>> 56);
        bytes[position + 1] = (byte) (value >>> 48);
        bytes[position + 2] = (byte) (value >>> 40);
        bytes[position + 3] = (byte) (value >>> 32);
        bytes[position + 4] = (byte) (value >>> 24);
        bytes[position + 5] = (byte) (value >>> 16);
        bytes[position + 6] = (byte) (value >>> 8);
        bytes[position + 7] = (byte) value;
    }

    static Optional<byte[]> insertValue(Optional<byte[]> previousValues, long newValue) {
        if (previousValues.isEmpty()) {
            byte[] value = new byte[8];
            Entry.writeLong(value, 0, newValue);
            return Optional.of(value);
        } else {
            var oldBytes = previousValues.get();
            var oldCount = oldBytes.length / 8;
            var buffer = ByteBuffer.wrap(oldBytes).asLongBuffer();
            var oldValues = new long[oldCount];
            buffer.get(oldValues);

            int i;
            for (i = 0; i < oldCount; i += 1) {
                if (oldValues[i] == newValue) {
                    return Optional.empty();
                }
                if (oldValues[i] > newValue) {
                    break;
                }
            }
            var newValues = new long[oldCount + 1];
            newValues[i] = newValue;
            System.arraycopy(oldValues, 0, newValues, 0, i);
            System.arraycopy(oldValues, i, newValues, i + 1, oldCount - i);

            var result = new byte[oldBytes.length + 8];
            var resultBuffer = ByteBuffer.wrap(result).asLongBuffer();
            resultBuffer.put(newValues);

            return Optional.of(result);
        }
    }
}
