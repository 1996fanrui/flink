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
import org.apache.flink.util.FileUtils;

import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Spill file I/O for the {@code filterAndRewrite} recovery path. Groups the writer, reader, and
 * chunk type as nested static classes so their tight coupling is visible at a glance.
 *
 * <p>Writer appends raw bytes; each logical entry is tracked in the corresponding Reader's entry
 * deque. Readers support both replay (readNext) and checkpoint snapshot (snapshot).
 */
@Internal
public final class FilteredSpillFile {

    private FilteredSpillFile() {}

    // -------------------------------------------------------------------------
    // Chunk — the payload unit returned by Reader.readNext()
    // -------------------------------------------------------------------------

    /**
     * A single spilled-data chunk returned by {@link Reader#readNext()}. The {@code data} array is
     * reused between calls on the same Reader; callers must consume bytes before the next readNext.
     */
    public static final class Chunk {

        private final InputChannelInfo channelInfo;
        private final byte[] data;
        private final int length;

        public Chunk(InputChannelInfo channelInfo, byte[] data, int length) {
            this.channelInfo = channelInfo;
            this.data = data;
            this.length = length;
        }

        public InputChannelInfo getChannelInfo() {
            return channelInfo;
        }

        /** Returns the internal data buffer; valid bytes are {@code [0, length)}. */
        public byte[] getData() {
            return data;
        }

        /** Number of valid bytes at the start of {@link #getData()}. */
        public int getLength() {
            return length;
        }
    }

    // -------------------------------------------------------------------------
    // Writer — pure disk appender; no cache, no emit callback
    // -------------------------------------------------------------------------

    /**
     * Appends raw bytes to spill files. Rotates to a new file when the current one exceeds 64 MB;
     * each rotation seals the outgoing Reader and opens a new one. All Readers are sealed on
     * {@link #finish()}.
     *
     * <p>Files are created lazily on the first {@link #writeEntry} call.
     */
    public static class Writer implements Closeable {

        private static final long FILE_ROTATION_THRESHOLD = 64L * 1024 * 1024; // 64 MB

        private final String[] spillDirs;
        private int currentDirIndex;
        private FileChannel currentChannel;
        private Path currentFilePath;
        private long currentFileOffset;
        private final List<Reader> readers;
        private boolean finished;

        /**
         * Creates a new Writer.
         *
         * @param spillDirs directories for writing spill files
         * @param memorySegmentSize max bytes per entry; retained for documentation / caller use
         * @throws IOException if spillDirs is empty
         */
        public Writer(String[] spillDirs, int memorySegmentSize) throws IOException {
            if (spillDirs.length == 0) {
                throw new IOException("Spill directories must not be empty");
            }
            this.spillDirs = spillDirs;
            this.currentDirIndex = 0;
            this.currentFileOffset = 0;
            this.readers = new ArrayList<>();
            this.finished = false;
        }

        /**
         * Appends {@code len} bytes from {@code data[off..off+len)} to the current spill file,
         * registering an entry for {@code channelInfo} in the current Reader. Lazily opens the first file;
         * rotates when the current file exceeds {@link #FILE_ROTATION_THRESHOLD}.
         */
        public void writeEntry(byte[] data, int off, int len, InputChannelInfo channelInfo)
                throws IOException {
            checkState(!finished, "writeEntry after finish");
            if (currentChannel == null) {
                openNewFile();
            } else if (currentFileOffset > FILE_ROTATION_THRESHOLD) {
                rotateFile();
            }
            long entryOffset = currentFileOffset;
            FileUtils.writeCompletely(currentChannel, ByteBuffer.wrap(data, off, len));
            currentFileOffset += len;
            currentReader().addEntry(channelInfo, entryOffset, len);
        }

        /** Seals the last Reader. After finish, no more writeEntry calls are accepted. */
        public void finish() {
            if (!finished) {
                finished = true;
                if (!readers.isEmpty()) {
                    readers.get(readers.size() - 1).seal();
                }
            }
        }

        /**
         * Finishes (if not already done), closes the write channel, and chain-closes all Readers.
         */
        @Override
        public void close() throws IOException {
            finish();
            try {
                if (currentChannel != null) {
                    currentChannel.close();
                    currentChannel = null;
                }
            } finally {
                for (Reader r : readers) {
                    r.close();
                }
            }
        }

        /** Returns true after {@link #finish()} has been called. */
        public boolean isFinished() {
            return finished;
        }

        /**
         * Returns true if no entries have been written yet. When idle, the dispatcher prefers P1
         * (direct buffer) over P2 (spill) to maintain ordering guarantees.
         */
        public boolean isIdle() {
            return readers.isEmpty();
        }

        /** Returns an unmodifiable view of all Readers created so far. */
        public List<Reader> getReaders() {
            return Collections.unmodifiableList(readers);
        }

        /** Deletes all spill files. Called after all data has been drained. */
        public void deleteAllFiles() {
            for (Reader r : readers) {
                try {
                    Files.deleteIfExists(r.filePath);
                } catch (IOException ignored) {
                    // best-effort cleanup
                }
            }
        }

        private Reader currentReader() {
            return readers.get(readers.size() - 1);
        }

        private void openNewFile() throws IOException {
            String dir = spillDirs[currentDirIndex];
            currentDirIndex = (currentDirIndex + 1) % spillDirs.length;
            Path dirPath = Paths.get(dir);
            Files.createDirectories(dirPath);
            currentFilePath = dirPath.resolve("spill-" + UUID.randomUUID() + ".bin");
            currentChannel =
                    FileChannel.open(
                            currentFilePath,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.WRITE);
            currentFileOffset = 0;
            readers.add(new Reader(currentFilePath));
        }

        private void rotateFile() throws IOException {
            // Seal the current Reader before opening a new file.
            currentReader().seal();
            currentChannel.close();
            currentChannel = null;
            openNewFile();
        }
    }

    // -------------------------------------------------------------------------
    // Reader — per-physical-file reader with entry deque and sealed state
    // -------------------------------------------------------------------------

    /**
     * Reads entries from a single spill file. Each instance is owned by exactly one consumer
     * thread: the original Reader by the replay path, a snapshot Reader by a checkpoint drain.
     *
     * <p>The internal buffer is reused across {@link #readNext()} calls; callers must consume each
     * Chunk before calling readNext again.
     */
    public static class Reader implements Closeable {

        private final FileChannel channel;
        final Path filePath; // accessed by Writer.deleteAllFiles
        private final Deque<Entry> entries = new ArrayDeque<>();
        private volatile boolean sealed = false;
        private byte[] buf;

        Reader(Path filePath) throws IOException {
            this.filePath = filePath;
            this.channel = FileChannel.open(filePath, StandardOpenOption.READ);
        }

        // ---- Write side (called by Writer) ----

        /** Registers an entry at {@code offset} with {@code length} bytes for {@code channelInfo}. */
        void addEntry(InputChannelInfo channelInfo, long offset, int length) {
            checkState(!sealed, "addEntry after seal");
            entries.addLast(new Entry(channelInfo, offset, length));
        }

        /** Seals this Reader; no more addEntry calls are allowed after this point. */
        void seal() {
            sealed = true;
        }

        public boolean isSealed() {
            return sealed;
        }

        // ---- Consume side (replay or checkpoint drain) ----

        /** Returns true if there are pending entries to consume. */
        public boolean hasEntries() {
            return !entries.isEmpty();
        }

        /**
         * Returns the channel of the next pending entry without consuming it, or null if empty.
         */
        public InputChannelInfo peekNextChannel() {
            Entry e = entries.peekFirst();
            return e != null ? e.channelInfo : null;
        }

        /**
         * Reads and returns the next pending entry as a {@link Chunk}. The Chunk's data array is
         * the Reader's internal buffer; it is overwritten by the next readNext call. Returns null
         * when there are no more entries.
         */
        public Chunk readNext() throws IOException {
            Entry entry = entries.pollFirst();
            if (entry == null) {
                return null;
            }
            if (buf == null || buf.length < entry.length) {
                buf = new byte[entry.length];
            }
            ByteBuffer bb = ByteBuffer.wrap(buf, 0, entry.length);
            long position = entry.offset;
            while (bb.hasRemaining()) {
                int n = channel.read(bb, position);
                if (n < 0) {
                    throw new IOException(
                            "Truncated spill file: "
                                    + entry.length
                                    + " bytes @"
                                    + entry.offset
                                    + " in "
                                    + filePath);
                }
                position += n;
            }
            return new Chunk(entry.channelInfo, buf, entry.length);
        }

        /**
         * Returns an independent Reader over the same file with a shallow copy of the current
         * entries. The snapshot is pre-sealed. Must be called only after this Reader is sealed.
         * The caller owns and must close the returned Reader.
         */
        public Reader snapshot() throws IOException {
            checkState(sealed, "snapshot requires sealed Reader");
            Reader snap = new Reader(filePath);
            snap.entries.addAll(this.entries);
            snap.sealed = true;
            return snap;
        }

        /** Returns the set of channels that still have pending entries. */
        public Set<InputChannelInfo> getPendingChannels() {
            Set<InputChannelInfo> channels = new HashSet<>();
            for (Entry e : entries) {
                channels.add(e.channelInfo);
            }
            return channels;
        }

        @Override
        public void close() throws IOException {
            channel.close();
        }

        // ---- Private entry metadata ----

        private static final class Entry {
            final InputChannelInfo channelInfo;
            final long offset;
            final int length;

            Entry(InputChannelInfo channelInfo, long offset, int length) {
                this.channelInfo = channelInfo;
                this.offset = offset;
                this.length = length;
            }
        }
    }
}
