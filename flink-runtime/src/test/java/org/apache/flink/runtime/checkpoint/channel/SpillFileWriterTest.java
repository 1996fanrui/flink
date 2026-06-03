/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;
import org.apache.flink.runtime.memory.MemoryManager;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

class SpillFileWriterTest {

    @TempDir Path tempDir;

    private static final int BUF_SIZE = MemoryManager.DEFAULT_PAGE_SIZE;

    @Test
    void testChannelSwitchFlushesAndKeepsEntryPerChannel() throws Exception {
        SpillFile spillFile = newSpillFile();
        try (SpillFileWriter writer = newWriter(spillFile)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);

            writeBytes(writer.requestBufferBlocking(c0), 100, (byte) 0x41);
            assertThat(readBack(spillFile)).isEmpty();

            Buffer slotForC1 = writer.requestBufferBlocking(c1);
            assertThat(readBack(spillFile)).containsExactly(new Record(c0, 100));

            writeBytes(slotForC1, 50, (byte) 0x42);
        }

        assertThat(readBack(spillFile))
                .containsExactly(
                        new Record(new InputChannelInfo(0, 0), 100),
                        new Record(new InputChannelInfo(0, 1), 50));
    }

    @Test
    void testBufferFullFlushesInsideRequest() throws Exception {
        SpillFile spillFile = newSpillFile();
        try (SpillFileWriter writer = newWriter(spillFile)) {
            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            writeBytes(writer.requestBufferBlocking(c0), BUF_SIZE, (byte) 0x55);
            assertThat(readBack(spillFile)).isEmpty();

            Buffer fresh = writer.requestBufferBlocking(c0);
            assertThat(readBack(spillFile)).containsExactly(new Record(c0, BUF_SIZE));
            assertThat(fresh.getSize()).isZero();
        }
    }

    @Test
    void testCloseFlushesResidualBytes() throws Exception {
        SpillFile spillFile = newSpillFile();
        SpillFileWriter writer = newWriter(spillFile);

        InputChannelInfo c0 = new InputChannelInfo(0, 0);
        Buffer slot = writer.requestBufferBlocking(c0);
        writeBytes(slot, 7, (byte) 0x33);
        assertThat(readBack(spillFile)).isEmpty();

        writer.close();
        assertThat(readBack(spillFile)).containsExactly(new Record(c0, 7));
    }

    @Test
    void testCloseIsIdempotent() throws Exception {
        SpillFile spillFile = newSpillFile();
        SpillFileWriter writer = newWriter(spillFile);

        writer.close();
        writer.close();
        writer.close();
        assertThat(readBack(spillFile)).isEmpty();
    }

    @Test
    void testGetSpillFileReturnsConstructorArg() {
        SpillFile spillFile = newSpillFile();
        SpillFileWriter writer = newWriter(spillFile);
        assertThat(writer.getSpillFile()).isSameAs(spillFile);
    }

    /** One spilled record's metadata, read back from disk. */
    private static final class Record {
        final InputChannelInfo channelInfo;
        final int length;

        Record(InputChannelInfo channelInfo, int length) {
            this.channelInfo = channelInfo;
            this.length = length;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof Record)) {
                return false;
            }
            Record that = (Record) o;
            return length == that.length && channelInfo.equals(that.channelInfo);
        }

        @Override
        public int hashCode() {
            return channelInfo.hashCode() * 31 + length;
        }

        @Override
        public String toString() {
            return channelInfo + ":" + length;
        }
    }

    private static List<Record> readBack(SpillFile spillFile) throws IOException {
        List<Record> out = new ArrayList<>();
        try (SpillFileReader reader = spillFile.reader()) {
            SpillFileReader.Chunk c;
            while ((c = reader.peek()) != null) {
                out.add(new Record(c.channelInfo, c.length));
                reader.advance();
            }
        }
        return out;
    }

    /**
     * Mirrors production: the owner holds a lifecycle grant for the file's lifetime so that
     * transient reader open/close pairs in a test never drop the ref count to zero (which would
     * delete the segments). Segment files are cleaned up by the temp dir.
     */
    private SpillFile newSpillFile() {
        // maxEntryLength matches the writer's buffer size, mirroring production where both equal
        // the network memory segment size, so a flushed record never exceeds the reader buffer.
        SpillFile spillFile = new SpillFile(tempDir, BUF_SIZE);
        spillFile.acquire();
        return spillFile;
    }

    private static SpillFileWriter newWriter(SpillFile spillFile) {
        return new SpillFileWriter(spillFile, newHeapBuffer(BUF_SIZE));
    }

    private static Buffer newHeapBuffer(int size) {
        MemorySegment seg = MemorySegmentFactory.allocateUnpooledSegment(size);
        return new NetworkBuffer(seg, MemorySegment::free);
    }

    private static void writeBytes(Buffer buf, int length, byte fill) {
        int writeAt = buf.getMemorySegmentOffset() + buf.getSize();
        byte[] data = new byte[length];
        Arrays.fill(data, fill);
        buf.getMemorySegment().put(writeAt, data);
        buf.setSize(buf.getSize() + length);
    }
}
