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
        // Writer holds a ref-count grant for the lifetime of the filter phase. Paired with the
        // release in close(); segments only get deleted once both this grant and the drain's grant
        // (taken by SpillFileReader's constructor) have been released, so drain can still read the
        // on-disk segments after filter completes.
        spillFile.acquire();
    }

    /** Returns the underlying {@link SpillFile} so the drain can read it post-close. */
    public SpillFile getSpillFile() {
        return spillFile;
    }

    /**
     * Package-private access to the underlying accumulator. The filter-phase wiring in {@code
     * RecoveredChannelStateHandler} passes it directly as the filter's {@code BufferSupplier} and
     * calls {@code beginChannel} before each filter invocation.
     */
    FilteredBufferWriter getAccumulator() {
        return accumulator;
    }

    /**
     * Closes the accumulator (flushing residual bytes) and releases the writer's ref-count grant on
     * the spill file. Segments are actually deleted only once the drain has also released its grant
     * — calling {@link SpillFile#release} instead of the forced {@link SpillFile#close} keeps the
     * on-disk segments alive for drain to read.
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
            spillFile.release();
        }
    }
}
