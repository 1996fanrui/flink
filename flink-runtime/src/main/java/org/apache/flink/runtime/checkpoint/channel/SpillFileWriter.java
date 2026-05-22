/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.annotation.Internal;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Filter-phase facade that pairs the {@link SpillFile} lifecycle with the {@link
 * FilteredBufferWriter} accumulator. The drain phase obtains the frozen {@link SpillFile} via
 * {@link #getSpillFile} after {@link #close} has been called.
 */
@Internal
public final class SpillFileWriter implements Closeable {

    private final SpillFile spillFile;
    private final FilteredBufferWriter accumulator;
    private boolean closed = false;

    public SpillFileWriter(SpillFile spillFile, FilteredBufferWriter accumulator) {
        this.spillFile = checkNotNull(spillFile);
        this.accumulator = checkNotNull(accumulator);
    }

    /** Returns the underlying {@link SpillFile} so the drain can read it post-close. */
    public SpillFile getSpillFile() {
        return spillFile;
    }

    /**
     * Package-private access to the underlying accumulator. The filter-phase wiring in {@code
     * RecoveredChannelStateHandler} passes it directly as the filter's {@code BufferSupplier}; the
     * filter tags each {@code requestBufferBlocking} call with the destination channel so the
     * accumulator flushes whenever the channel switches.
     */
    FilteredBufferWriter getAccumulator() {
        return accumulator;
    }

    /**
     * Closes the accumulator (flushing residual bytes). Does not touch the {@link SpillFile}
     * lifecycle: the producer (the handler that constructed this writer) holds the only initial
     * ref-count grant on the SpillFile, and that grant must outlive this close — the drain runs
     * later on a different thread and needs the on-disk segments to still exist. The producer grant
     * is transferred to the {@code SpillFileReader} at handoff time; segments are deleted only when
     * both the producer-transferred grant and the drain's grant have been released.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        accumulator.close();
    }
}
