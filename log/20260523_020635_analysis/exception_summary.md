# Failure Exception Summary

- Source log: `log/20260523_020635.log`
- Split directory: `log/20260523_020635_analysis/split_failures`
- Total failed test cases: **25**
- Distinct top-level exception classes: **2**
- Distinct root-cause classes (deepest `Caused by:`): **2**

## By root cause (deepest `Caused by:`)

| Root cause | Count | Sample first line |
| --- | ---: | --- |
| `java.lang.IllegalArgumentException` | 14 | java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value abcdea0000001100 |
| `java.io.EOFException` | 11 | java.io.EOFException: java.io.EOFException |

## By top-level exception (what JUnit reported)

| Top exception | Count | Sample first line |
| --- | ---: | --- |
| `org.opentest4j.AssertionFailedError` | 23 | org.opentest4j.AssertionFailedError: Graph is in globally terminal state (FAILED) |
| `org.apache.flink.runtime.client.JobExecutionException` | 2 | org.apache.flink.runtime.client.JobExecutionException: Job execution failed. |

## Test cases grouped by root cause

### `java.lang.IllegalArgumentException` (14)

- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale keyed_different_parallelism from 12 to 7, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value abcdea0000001100
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale multi_input from 3 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value abcdea0000001100
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale multi_input from 7 to 3, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value abcdea0000001100
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale pipeline from 2 to 1, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 11000000000000
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale pipeline from 7 to 3, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 20f66abcd
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale pipeline from 8 to 4, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value abcdea0000001100
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale union from 7 to 3, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 4d87babcd
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale keyed_different_parallelism from 7 to 12, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 11000000000000
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale multi_input from 2 to 3, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 1a395abcdea
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale pipeline from 1 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 11000000000000
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale pipeline from 2 to 3, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 35aaabcd
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale pipeline from 20 to 21, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 11000000000000
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale union from 1 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.lang.IllegalArgumentException: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value 11000000000000
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale union from 3 to 7, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.apache.flink.runtime.client.JobExecutionException`
    - root first line: java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value abcdea0000001100

### `java.io.EOFException` (11)

- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale broadcast from 5 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale keyed_broadcast from 7 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale multi_input from 2 to 1, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale pipeline from 21 to 20, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale pipeline from 3 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[downscale union from 2 to 1, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale broadcast from 2 to 5, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.apache.flink.runtime.client.JobExecutionException`
    - root first line: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale keyed_broadcast from 2 to 7, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale multi_input from 1 to 2, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale multi_input from 3 to 7, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
- `UnalignedCheckpointRescaleITCase_shouldRescaleUnalignedCheckpoint[upscale pipeline from 3 to 7, sourceSleepMs = 0]_from_20260523_020635.log`
    - top: `org.opentest4j.AssertionFailedError`
    - root first line: java.io.EOFException: java.io.EOFException
