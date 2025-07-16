package org.apache.flink.runtime.state;

import java.io.Serializable;

/**
 * Describes the consumption progress within a logical stream of buffers.
 * This is an immutable value object used for state snapshots.
 */
public final class ConsumptionProgress implements Serializable, Comparable<ConsumptionProgress> {
    private static final long serialVersionUID = 1L;

    /**
     * The index of the buffer in the logical sequence of its channel.
     * This acts as a unique, ordered ID for the buffer.
     */
    private final int bufferIndex;

    /**
     * The byte offset within the buffer, up to which all data has been consumed.
     * A value of 0 means this buffer has not been consumed yet.
     */
    private final int offsetInBuffer;

    public ConsumptionProgress(int bufferIndex, int offsetInBuffer) {
        this.bufferIndex = bufferIndex;
        this.offsetInBuffer = offsetInBuffer;
    }

    public int getBufferIndex() {
        return bufferIndex;
    }

    public int getOffsetInBuffer() {
        return offsetInBuffer;
    }

    @Override
    public int compareTo(ConsumptionProgress other) {
        int bufferIndexDiff = Integer.compare(this.bufferIndex, other.bufferIndex);
        if (bufferIndexDiff != 0) {
            return bufferIndexDiff;
        }
        return Integer.compare(this.offsetInBuffer, other.offsetInBuffer);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ConsumptionProgress that = (ConsumptionProgress) o;
        return bufferIndex == that.bufferIndex && offsetInBuffer == that.offsetInBuffer;
    }

    @Override
    public int hashCode() {
        return 31 * bufferIndex + offsetInBuffer;
    }

    @Override
    public String toString() {
        return "Progress{" +
                "buffer=" + bufferIndex +
                ", offset=" + offsetInBuffer +
                '}';
    }
}
