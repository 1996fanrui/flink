<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at
  http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Design Doc: In-flight Data Rescaling Descriptor for Channels Without State

## 1. Context and Problem

This document details the design considerations for fixing a bug in `TaskStateAssignment#createGateOrPartitionRescalingDescriptors`.

The core problem is that the recovery logic attempts to generate rescaling descriptors for *all* channels of a vertex if *any* channel has unaligned checkpoint (UC) state. This fails when a channel uses a partitioner that does not support rescaling (e.g., `forward`), which correctly has no UC state, but is part of a vertex that is being restored. The attempt to calculate a mapping for this stateless channel results in an `UnsupportedOperationException`.

The solution is to make the descriptor generation process aware of the state on a per-channel basis. For channels that have no UC state, we must return a special descriptor indicating that no action is needed. This document records the design evolution for this special descriptor.

## 2. Design Evolution and Final Rationale

### 2.1. Initial Idea: A Simple, Passive Constant

The first idea was to create a simple `NO_STATE` constant that was a standard `InflightDataGateOrPartitionRescalingDescriptor` constructed with empty/identity values.

-   **Concern:** This approach is not safe. If a future code modification accidentally calls a getter method like `getOldSubtaskInstances()` on this object, it would return an empty array. The logic would then proceed silently, potentially causing data loss or state corruption, which is a more dangerous failure mode than a loud crash.

### 2.2. Final Design: A Behavior-Compatible AND Fail-Fast Singleton

To address the risk of silent failures while ensuring the solution is clean, the final design incorporates two key principles: **Behavioral Compatibility** and **Fail-Fast Safety**.

-   **Principle 1: Behavioral Compatibility:** The `NO_STATE` object must integrate seamlessly with existing logic without requiring new checks (like `hasState()`). The key existing logic is the check `Arrays.stream(...).allMatch(d -> d.isIdentity())` in `createRescalingDescriptor`. Therefore, `NO_STATE.isIdentity()` **must** return `true`.

-   **Principle 2: Fail-Fast Safety:** To prevent future bugs from causing silent errors, any attempt to actually *use* the data from a `NO_STATE` object must result in an immediate, clear failure. This means methods like `getOldSubtaskInstances()` and `getRescaleMappings()` should throw an exception.

-   **Implementation:** The final design achieves both goals by implementing `NO_STATE` as an instance of an anonymous subclass:

    ```java
    // To be added inside InflightDataGateOrPartitionRescalingDescriptor class

    public static final InflightDataGateOrPartitionRescalingDescriptor NO_STATE =
        new InflightDataGateOrPartitionRescalingDescriptor(
            new int[0],
            RescaleMappings.identity(),
            java.util.Collections.emptySet(),
            MappingType.IDENTITY) { // Principle 1: isIdentity() will return true

            @Override
            public int[] getOldSubtaskInstances() {
                // Principle 2: Fail-fast if misused
                throw new UnsupportedOperationException(
                    "Cannot get old subtasks from a descriptor that represents no state.");
            }

            @Override
            public RescaleMappings getRescaleMappings() {
                // Principle 2: Fail-fast if misused
                throw new UnsupportedOperationException(
                    "Cannot get rescale mappings from a descriptor that represents no state.");
            }
        };
    ```

### 3. Conclusion

This hybrid design is the most robust solution. It correctly fixes the original bug by allowing stateless channels to be represented as "identity" operations. Simultaneously, it provides a critical safety net that protects against future regressions by ensuring any accidental attempt to process a stateless channel results in an immediate and clear exception.
