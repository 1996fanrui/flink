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
import org.apache.flink.runtime.io.network.buffer.Buffer;

import java.io.Closeable;
import java.io.IOException;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Filter-phase facade that pairs the {@link SpillFile} lifecycle with the {@link
 * FilteredBufferWriter} accumulator. Callers in the filter phase see exactly one entry point
 * ({@link #write}) and one teardown point ({@link #close}); the drain phase obtains the frozen
 * {@link SpillFile} via {@link #getSpillFile} after close has been called.
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
     * RecoveredChannelStateHandler} needs the accumulator's stable pre-filter buffer to supply the
     * filter's output allocator.
     */
    FilteredBufferWriter getAccumulator() {
        return accumulator;
    }

    /**
     * Delegates a filtered-output record to the accumulator. The caller retains buffer ownership;
     * the accumulator copies the readable bytes.
     */
    public void write(InputChannelInfo channelInfo, Buffer buf)
            throws IOException, InterruptedException {
        accumulator.write(channelInfo, buf);
    }

    /**
     * Closes the accumulator first (flushing residual bytes and itself closing the spill file),
     * then defensively closes the spill file again. The second call is a no-op because {@link
     * SpillFile#close} is idempotent, but it leaves the close-ordering invariant intentional and
     * explicit in the facade.
     */
    @Override
    public void close() throws IOException {
        if (closed) {
            return;
        }
        closed = true;
        try {
            accumulator.close();
        } finally {
            spillFile.close();
        }
    }
}
