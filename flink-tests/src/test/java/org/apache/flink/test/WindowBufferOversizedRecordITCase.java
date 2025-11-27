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

package org.apache.flink.test.checkpointing;

import org.apache.flink.configuration.AlgorithmOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.concurrent.TimeUnit;

/**
 * Test to reproduce StackOverflowError in RecordsWindowBuffer when a single record exceeds the
 * buffer capacity.
 *
 * <h2>Bug Reproduction Strategy</h2>
 *
 * <p>The RecordsWindowBuffer.addElement() method has a flawed assumption:
 *
 * <pre>{@code
 * try {
 *     recordsBuffer.append(lookup, recordSerializer.toBinaryRow(element));
 * } catch (EOFException e) {
 *     flush();
 *     addElement(key, sliceEnd, element);  // recursive call
 * }
 * }</pre>
 *
 * <p>The code assumes EOFException means "buffer is full, flush will fix it". However, if a single
 * record is larger than the entire buffer capacity:
 *
 * <ol>
 *   <li>append() throws EOFException (record too large)
 *   <li>flush() clears the buffer
 *   <li>recursive addElement() tries again
 *   <li>append() throws EOFException again (record still too large)
 *   <li>Infinite recursion → StackOverflowError
 * </ol>
 *
 * <h2>Test Configuration</h2>
 *
 * <ul>
 *   <li>LOCAL_AGG_BUFFER_SIZE: 512KB (very small buffer for LocalWindowAggregate)
 *   <li>GLOBAL_AGG_BUFFER_SIZE: 512KB (very small buffer for GlobalWindowAggregate)
 *   <li>Record Size: 1MB (larger than buffer)
 *   <li>Expected: StackOverflowError
 * </ul>
 */
@ExtendWith(TestLoggerExtension.class)
class WindowBufferOversizedRecordITCase {

    /**
     * Test that a single oversized record causes StackOverflowError in LocalWindowAggregate.
     *
     * <p>This test uses LOCAL_AGG_BUFFER_SIZE to limit the buffer size for LocalWindowAggregate
     * (the first phase of two-phase aggregation).
     */
    @Test
    void testOversizedRecordInLocalAggregate() throws Exception {
        Configuration config = new Configuration();
        // Set a very small buffer size (512KB) for local aggregation
        config.set(AlgorithmOptions.LOCAL_AGG_BUFFER_SIZE, MemorySize.parse("512k"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create source with oversized record (1MB string, larger than 512KB buffer)
        tEnv.executeSql(
                "CREATE TABLE source_local (\n"
                        + "  id STRING,\n"
                        + "  huge_data STRING,\n"
                        + "  event_time TIMESTAMP(3),\n"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'number-of-rows' = '1',\n"
                        + "  'fields.id.length' = '10',\n"
                        // 1MB string - this exceeds the 512KB buffer capacity
                        + "  'fields.huge_data.length' = '1048576'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_local (\n"
                        + "  id STRING,\n"
                        + "  max_data STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ")");

        // Window aggregation triggers LocalWindowAggregate (first phase)
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_local\n"
                                + "SELECT\n"
                                + "  id,\n"
                                + "  MAX(huge_data) AS max_data\n"
                                + "FROM TABLE(\n"
                                + "  TUMBLE(TABLE source_local, DESCRIPTOR(event_time), INTERVAL '1' HOUR)\n"
                                + ")\n"
                                + "GROUP BY window_start, window_end, id");

        verifyStackOverflowError(result, "LocalWindowAggregate");
    }

    /**
     * Test that a single oversized record causes StackOverflowError in GlobalWindowAggregate.
     *
     * <p>This test uses GLOBAL_AGG_BUFFER_SIZE to limit the buffer size for GlobalWindowAggregate
     * (the second phase of two-phase aggregation).
     */
    @Test
    void testOversizedRecordInGlobalAggregate() throws Exception {
        Configuration config = new Configuration();
        // Set a very small buffer size (512KB) for global aggregation
        config.set(AlgorithmOptions.GLOBAL_AGG_BUFFER_SIZE, MemorySize.parse("512k"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create source with oversized record (1MB string, larger than 512KB buffer)
        tEnv.executeSql(
                "CREATE TABLE source_global (\n"
                        + "  id STRING,\n"
                        + "  huge_data STRING,\n"
                        + "  event_time TIMESTAMP(3),\n"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'number-of-rows' = '1',\n"
                        + "  'fields.id.length' = '10',\n"
                        // 1MB string - this exceeds the 512KB buffer capacity
                        + "  'fields.huge_data.length' = '1048576'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_global (\n"
                        + "  id STRING,\n"
                        + "  max_data STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ")");

        // Window aggregation triggers GlobalWindowAggregate (second phase)
        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_global\n"
                                + "SELECT\n"
                                + "  id,\n"
                                + "  MAX(huge_data) AS max_data\n"
                                + "FROM TABLE(\n"
                                + "  TUMBLE(TABLE source_global, DESCRIPTOR(event_time), INTERVAL '1' HOUR)\n"
                                + ")\n"
                                + "GROUP BY window_start, window_end, id");

        verifyStackOverflowError(result, "GlobalWindowAggregate");
    }

    /**
     * Test with both LOCAL and GLOBAL buffer sizes set to small values.
     *
     * <p>This ensures the bug is triggered regardless of which phase processes the oversized
     * record.
     */
    @Test
    void testOversizedRecordWithBothBuffersSmall() throws Exception {
        Configuration config = new Configuration();
        // Set both buffer sizes to be very small
        config.set(AlgorithmOptions.LOCAL_AGG_BUFFER_SIZE, MemorySize.parse("512k"));
        config.set(AlgorithmOptions.GLOBAL_AGG_BUFFER_SIZE, MemorySize.parse("512k"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(config);
        env.setParallelism(1);
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);

        // Create source with oversized record
        tEnv.executeSql(
                "CREATE TABLE source_both (\n"
                        + "  id STRING,\n"
                        + "  huge_data STRING,\n"
                        + "  event_time TIMESTAMP(3),\n"
                        + "  WATERMARK FOR event_time AS event_time - INTERVAL '1' SECOND\n"
                        + ") WITH (\n"
                        + "  'connector' = 'datagen',\n"
                        + "  'rows-per-second' = '1',\n"
                        + "  'number-of-rows' = '1',\n"
                        + "  'fields.id.length' = '10',\n"
                        + "  'fields.huge_data.length' = '1048576'\n"
                        + ")");

        tEnv.executeSql(
                "CREATE TABLE sink_both (\n"
                        + "  id STRING,\n"
                        + "  max_data STRING\n"
                        + ") WITH (\n"
                        + "  'connector' = 'blackhole'\n"
                        + ")");

        TableResult result =
                tEnv.executeSql(
                        "INSERT INTO sink_both\n"
                                + "SELECT\n"
                                + "  id,\n"
                                + "  MAX(huge_data) AS max_data\n"
                                + "FROM TABLE(\n"
                                + "  TUMBLE(TABLE source_both, DESCRIPTOR(event_time), INTERVAL '1' HOUR)\n"
                                + ")\n"
                                + "GROUP BY window_start, window_end, id");

        verifyStackOverflowError(result, "Both Local and Global");
    }

    private void verifyStackOverflowError(TableResult result, String testCase) throws Exception {
        try {
            result.await(60, TimeUnit.SECONDS);
            System.out.println(
                    "WARNING: ["
                            + testCase
                            + "] Job completed without StackOverflowError. "
                            + "Bug may not have been triggered.");
        } catch (Exception e) {
            Throwable cause = e;
            boolean foundStackOverflow = false;
            while (cause != null) {
                if (cause instanceof StackOverflowError) {
                    foundStackOverflow = true;
                    System.out.println(
                            "SUCCESS: [" + testCase + "] StackOverflowError reproduced!");
                    break;
                }
                cause = cause.getCause();
            }
            if (!foundStackOverflow) {
                System.out.println("[" + testCase + "] Job failed with: " + e.getMessage());
                throw e;
            }
        }
    }
}
