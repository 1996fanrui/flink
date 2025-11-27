# RecordsWindowBuffer StackOverflowError Bugfix

## Problem

`RecordsWindowBuffer.addElement()` catches `EOFException` and retries by calling itself recursively:

```java
catch (EOFException e) {
    flush();
    addElement(key, sliceEnd, element);  // recursive retry
}
```

If the retry keeps failing, this causes infinite recursion → **StackOverflowError**.

## Reproduction Test

**Test class**: `WindowBufferOversizedRecordITCase`

**How it reproduces the bug**:
- Sets buffer size to 512KB (`LOCAL_AGG_BUFFER_SIZE` / `GLOBAL_AGG_BUFFER_SIZE`)
- Generates a single record with 1MB data (larger than buffer)
- Record can never fit → infinite retry → StackOverflowError

After the fix, this test should throw `EOFException` instead of `StackOverflowError`.

## Core Idea

The retry mechanism exists to handle the "buffer full" case.

**For a single element, retry should happen at most once. Why?**

1. `flush()` resets the buffer to empty before retrying
2. If retry still fails with an empty buffer, further retries are pointless
3. They will fail for the same reason

## Solution

**Explicitly limit retry to at most once** — no recursion, use a simple loop with clear control:

```java
@Override
public void addElement(RowData key, long sliceEnd, RowData element) throws Exception {
    minSliceEnd = Math.min(sliceEnd, minSliceEnd);
    reuseWindowKey.replace(sliceEnd, key);

    boolean retried = false;
    while (true) {
        LookupInfo<WindowKey, Iterator<RowData>> lookup = recordsBuffer.lookup(reuseWindowKey);
        try {
            recordsBuffer.append(lookup, recordSerializer.toBinaryRow(element));
            break;  // success, exit loop
        } catch (EOFException e) {
            if (retried) {
                // Already retried once, don't retry again
                throw e;
            }
            flush();
            retried = true;
            // continue loop to retry once
        }
    }

    if (recordsBuffer.getNumElements() >= maxBufferedElements) {
        flush();
    }
}
```

**Why this is better**:
- Clear and explicit: "retry at most once" is directly visible in code
- No recursion, no stack growth
- Easy to understand and maintain

## Why This Works

**Example: 11 records, buffer can hold 10**

| Scenario | retried flag | Behavior | Result |
|----------|--------------|----------|--------|
| Record #11 fails since buffer full | false | flush → retry | ✅ Retry succeeds |
| Record #11 fails since record too large | false → true | flush → retry → fail again | retried=true → throw |
| Record #1 fails since record too large | false → true | flush → retry → fail again | retried=true → throw |

## Benefits

1. **Prevents StackOverflowError**: No recursion at all
2. **Preserves normal behavior**: Buffer full case still works (flush + retry once)
3. **Better diagnostics**: Exception is thrown with full stack trace
4. **Clear intent**: Code explicitly shows "retry at most once"
