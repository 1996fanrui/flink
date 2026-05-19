# InputChannel-side changes

> Scope: when `checkpointingDuringRecoveryEnabled=true` + filter is on, this doc covers the entry point through which the drain phase delivers a recovered buffer into a physical `InputChannel`. When the feature is off, master is not touched.

## 1. Design principles

- Once a recovered buffer enters a physical channel, the downstream path (enqueue → notify the task to consume → task consumption) is **identical** to that of an upstream network buffer.
- The master wake-up chain `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` has **zero changes**.
- No new branch is introduced on the `InputChannel.getNextBuffer` hot path (other than the minimal unavoidable change in each §3 candidate).
- The delivery action must happen inside `Unspiller.monitor` (see the strong principles in [`coordination.md`](./coordination.md)).

## 2. master asymmetry today

| Channel | Existing push entry | Can drain reuse it directly? |
|---|---|---|
| `RemoteInputChannel` | `onBuffer(Buffer, sequenceNumber, backlog, subpartitionId)` — internally does `synchronized(receivedBuffers) { add } + notifyChannelNonEmpty()`, which is exactly what we want | **Not directly**: the same method also performs sequence-number validation / priority-event branching / `channelStatePersister.checkForBarrier+maybePersist` / `onSenderBacklog` and other network-protocol-side bookkeeping; these side effects must not fire on the recovery drain path |
| `LocalInputChannel` | **None**; inherits `BufferAvailabilityListener`, buffers are pulled by `subpartitionView.getNextBuffer()`; the channel has no `receivedBuffers` field at all | **No ready-made interface**; new mechanism is required |

So "just call the existing add-buffer interface" is partly feasible for Remote (by bypassing the network bookkeeping) and not feasible at all for Local.

## 3. Landing candidates (TBD)

| Option | Shape | Pros / cons |
|---|---|---|
| **A** | Introduce `onRecoveredStateBuffer(Buffer)` on the `InputChannel` base class; the method body is the equivalent of master's `RecoveredInputChannel.onRecoveredStateBuffer` (`synchronized(receivedBuffers) { add } + notifyChannelNonEmpty`). Add a `receivedBuffers` queue on Local and weave it into `getNextBuffer`'s priority. | Most symmetric; changes concentrated; master's existing path untouched. Local needs a "check the recovered queue first" prefix added to `getNextBuffer`. |
| **B** | Remote reuses the `onBuffer` core + Local separately adds push: extract the "`receivedBuffers.add + notifyChannelNonEmpty`" core from `RemoteInputChannel.onBuffer` into a package-private method that drain calls; `LocalInputChannel` still needs a mechanism similar to A. | Asymmetric structure; Remote change is small but Local change equals A; overall complexity is actually higher. |
| **C** | Wrap a `ResultSubpartitionView` wrapper: drain hands the recovered buffer to the wrapper view, and the channel still walks the master pull path. | The channel itself is untouched; but this intrudes on the `ResultSubpartitionView` abstraction, with a larger blast radius than A/B. |

**This doc does not lock the option; the decision will be filled in after discussion.** Initial preference is A: it aligns best with the design principle "recovered buffer is equivalent to a network buffer", and the diff lands on two files (`InputChannel` / `LocalInputChannel`) that a reviewer can locate at a glance.

## 4. Invariants (regardless of which option is chosen)

- The existing callers of `getNextBuffer` (`InputGate.pollNext`, `StreamTaskNetworkInput`, etc.) keep the same signature and contract.
- The `notifyChannelNonEmpty / queueChannel / inputChannelsWithData` chain is unchanged.
- The priority-event chain (`addPriorityBuffer / firstPriorityEvent`) is unchanged.
- The existing `onRecoveredStateBuffer` on `RecoveredInputChannel` remains valid on the filter-off path and is not touched.
