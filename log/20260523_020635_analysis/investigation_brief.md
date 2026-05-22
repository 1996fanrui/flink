# Investigation Brief — 20260523_020635 run

## Background

- Branch: `38544-spilling-v2/20260522-02-poc-address-comments` (FLINK-38544)
- Design family: [`requirements/38544/`](../../requirements/38544/)
  - **Core design**: [`requirements/38544/simplify_approach/`](../../requirements/38544/simplify_approach/) — the spec for replacing the heap-fallback path with a disk-spill path during `checkpointing during recovery + filter`. Key components: `SpillFileWriter` / `SpillFileReader`, `channelIOExecutor` drain task, lock + 3-step checkpoint protocol (see `overview.md`, `input_channel.md`, `unspiller.md`, `coordination.md`).
  - **Previous-round findings**: [`requirements/38544/code_review/`](../../requirements/38544/code_review/) (round1…round5). The current branch is the implementation that landed those fixes.
- Single goal of this run: investigate why `UnalignedCheckpointRescaleITCase.shouldRescaleUnalignedCheckpoint` still fails 25 times on this branch.

## The two reported exceptions are almost certainly one root cause

| Root cause class | Count | Where it surfaces |
| --- | ---: | --- |
| `java.lang.IllegalArgumentException: Stream corrupted. Cannot find the header abcdeafc00000000 in the value <hex>` | 14 | Deserializing a buffer that was read back from the spill file. |
| `java.io.EOFException` | 11 | Reading past the end of the spilled stream before deserialization can finish. |

Both fire on the **read-back side** (drain / unspill) of the new write-then-replay path:

- The "stream corrupted, header `abcdeafc00000000` missing" message is the buffer / segment magic-header check failing — the bytes the reader pulled out of the spill stream are not the bytes the writer was supposed to have put in.
- `EOFException` is the same failure mode one step earlier in the pipeline: the reader runs out of bytes before it can even reach the header check.

A misaligned write (wrong length / wrong offset / truncated payload / overlapping segment reuse) shows up as `EOFException` when the deficit lands inside a length-prefixed read, and as `Stream corrupted` when there are still bytes left but they are the *wrong* bytes (header mismatch). Same family of bug, two surface manifestations depending on exactly where the read pointer ends up.

The parameter spread in the failing cases (broadcast / keyed_broadcast / keyed_different_parallelism / pipeline / union / multi_input, both up- and downscale) also points at the spill read/write contract rather than any one topology — it is the path the buffers travel through, not the shape of the job.

## Suggested entry points

When triaging the split logs in `split_failures/`, start from the writer↔reader contract:

1. `SpillFileWriter` flush ordering vs. `SpillFileReader` framing — does every buffer the writer commits carry the same length / header layout the reader expects?
2. Buffer / `MemorySegment` lifecycle around the spill path — is any segment recycled or overwritten between "written to disk" and "read back" (recall the recent `RecoveredInputChannel` drain-keepalive fix, commit `b68b8db2813`)?
3. `channelIOExecutor` drain task vs. checkpoint 3-step protocol — see `coordination.md`. A drain that races a checkpoint barrier could mis-order or truncate the replay stream.

The full per-test split logs are in `split_failures/`; the exception class for each is enumerated in `exception_summary.md` / `exception_summary.json`.
