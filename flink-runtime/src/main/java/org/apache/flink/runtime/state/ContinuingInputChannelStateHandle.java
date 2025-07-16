package org.apache.flink.runtime.state;

import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;

/**
 * A state handle for an input channel that is still in the process of recovering
 * from a previous checkpoint. This is a counterpart to {@link InputChannelStateHandle}.
 */
public final class ContinuingInputChannelStateHandle implements InputStateHandle {
    private static final long serialVersionUID = 1L;

    private final InputChannelInfo info;
    private final RecoveryContinuationHandle delegate;

    public ContinuingInputChannelStateHandle(InputChannelInfo info, RecoveryContinuationHandle delegate) {
        this.info = info;
        this.delegate = delegate;
    }

    @Override
    public InputChannelInfo getInfo() {
        return info;
    }

    @Override
    public StreamStateHandle getDelegate() {
        return delegate;
    }

    @Override
    public StateObjectType getType() {
        // We can define a new type, or reuse an existing one if it fits.
        // For clarity, let's assume a new type is needed.
        // This requires adding a new enum constant to StateObjectType.
        // For now, we can conceptually think of it as a new type.
        // Let's return a placeholder for now.
        return StateObjectType.RECOVERY_CONTINUATION;
    }

    @Override
    public void discardState() throws Exception {
        delegate.discardState();
    }

    @Override
    public long getStateSize() {
        return delegate.getStateSize();
    }
}
