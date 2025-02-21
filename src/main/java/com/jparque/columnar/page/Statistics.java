package com.jparque.columnar.page;

import java.nio.ByteBuffer;

/**
 * Represents statistics for a column chunk or page.
 * Statistics include information like min/max values, null count, etc.
 */
public class Statistics {
    private final ByteBuffer minValue;
    private final ByteBuffer maxValue;
    private final long nullCount;
    private final long distinctCount;

    public Statistics(ByteBuffer minValue, ByteBuffer maxValue, long nullCount, long distinctCount) {
        this.minValue = minValue != null ? minValue.duplicate() : null;
        this.maxValue = maxValue != null ? maxValue.duplicate() : null;
        this.nullCount = nullCount;
        this.distinctCount = distinctCount;
    }

    public int getSize() {
        // Size calculation:
        // - 4 bytes for min value length (if present)
        // - min value bytes (if present)
        // - 4 bytes for max value length (if present)
        // - max value bytes (if present)
        // - 8 bytes for null count
        // - 8 bytes for distinct count
        int size = 16; // null count + distinct count
        if (minValue != null) {
            size += 4 + minValue.remaining();
        }
        if (maxValue != null) {
            size += 4 + maxValue.remaining();
        }
        return size;
    }

    public void write(ByteBuffer buffer) {
        if (minValue != null) {
            buffer.putInt(minValue.remaining());
            buffer.put(minValue.duplicate());
        } else {
            buffer.putInt(0);
        }

        if (maxValue != null) {
            buffer.putInt(maxValue.remaining());
            buffer.put(maxValue.duplicate());
        } else {
            buffer.putInt(0);
        }

        buffer.putLong(nullCount);
        buffer.putLong(distinctCount);
    }

    public ByteBuffer getMinValue() {
        return minValue != null ? minValue.duplicate() : null;
    }

    public ByteBuffer getMaxValue() {
        return maxValue != null ? maxValue.duplicate() : null;
    }

    public long getNullCount() {
        return nullCount;
    }

    public long getDistinctCount() {
        return distinctCount;
    }
}
