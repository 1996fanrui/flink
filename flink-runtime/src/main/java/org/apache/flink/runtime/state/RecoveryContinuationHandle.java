package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;

import java.io.IOException;
import java.util.Map;

/**
 * A special StreamStateHandle that does not point to a data stream,
 * but instead contains metadata required to continue a recovery process.
 */
public final class RecoveryContinuationHandle implements StreamStateHandle {
    private static final long serialVersionUID = 1L;

    private final long originalCheckpointId;
    private final ResultPartitionID originalPartitionId;
    private final Map<KeyGroupRange, ConsumptionProgress> absoluteKeyGroupRangeProgress;

    public RecoveryContinuationHandle(
            long originalCheckpointId,
            ResultPartitionID originalPartitionId,
            Map<KeyGroupRange, ConsumptionProgress> absoluteKeyGroupRangeProgress) {
        this.originalCheckpointId = originalCheckpointId;
        this.originalPartitionId = originalPartitionId;
        this.absoluteKeyGroupRangeProgress = absoluteKeyGroupRangeProgress;
    }

    public long getOriginalCheckpointId() {
        return originalCheckpointId;
    }

    public ResultPartitionID getOriginalPartitionId() {
        return originalPartitionId;
    }

    public Map<KeyGroupRange, ConsumptionProgress> getAbsoluteKeyGroupRangeProgress() {
        return absoluteKeyGroupRangeProgress;
    }

    @Override
    public void discardState() throws Exception {
        // No-op, as this handle does not own any physical resources.
    }

    @Override
    public long getStateSize() {
        // A more accurate implementation would serialize the object to get the exact size.
        // For now, returning 0 is acceptable as it's metadata-only.
        return 0;
    }

    @Override
    public FSDataInputStream openInputStream() throws IOException {
        throw new UnsupportedOperationException(
                "This is a metadata-only handle and does not have a data stream.");
    }
    
    @Override
    public String toString() {
        return "RecoveryContinuationHandle{" +
                "originalCheckpointId=" + originalCheckpointId +
                ", originalPartitionId=" + originalPartitionId +
                ", progressEntries=" + absoluteKeyGroupRangeProgress.size() +
                '}';
    }
}
