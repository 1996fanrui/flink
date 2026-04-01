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

import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.util.FileUtils;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Writes raw buffer byte data to a spill file. No metadata (length prefix, channel context,
 * DataType) is written to disk; all metadata is maintained in memory by {@link
 * SpillingBufferManager}.
 */
class SpillFileWriter implements Closeable {

    private final File file;
    private final FileChannel fileChannel;
    private long bytesWritten;

    SpillFileWriter(File file) throws IOException {
        this.file = checkNotNull(file, "file");
        this.fileChannel =
                FileChannel.open(
                        file.toPath(),
                        StandardOpenOption.CREATE,
                        StandardOpenOption.WRITE,
                        StandardOpenOption.TRUNCATE_EXISTING);
    }

    void writeBuffer(Buffer buffer) throws IOException {
        int dataLength = buffer.readableBytes();
        FileUtils.writeCompletely(fileChannel, buffer.getNioBufferReadable());
        bytesWritten += dataLength;
    }

    long getBytesWritten() {
        return bytesWritten;
    }

    File getFile() {
        return file;
    }

    @Override
    public void close() throws IOException {
        fileChannel.close();
    }
}
