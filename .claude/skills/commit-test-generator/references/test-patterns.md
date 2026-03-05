# Flink Test Patterns Reference

## Common Test Base Classes

### For Runtime Tests
```java
import org.apache.flink.runtime.testutils.AbstractTestBase;

public class MyTest extends AbstractTestBase {
    // Provides miniCluster setup/teardown
}
```

### For Streaming Tests
```java
import org.apache.flink.streaming.util.TestHarnessUtil;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

// For testing operators
OneInputStreamOperatorTestHarness<IN, OUT> harness =
    new OneInputStreamOperatorTestHarness<>(operator);
```

### For Table/SQL Tests
```java
import org.apache.flink.table.planner.utils.TableTestBase;

public class MySQLTest extends TableTestBase {
    // Provides table environment setup
}
```

## Test Naming Conventions

```java
// Test method names should describe what is being tested
@Test
public void testNormalOperation() { }

@Test
public void testExceptionHandling() { }

@Test
public void testEdgeCase_NullInput() { }

@Test
public void testConcurrency_MultipleThreads() { }
```

## Common Test Patterns

### 1. State Backend Tests
```java
@Test
public void testStateBackendCheckpointing() throws Exception {
    StateBackend backend = new RocksDBStateBackend("file:///tmp");

    try (CheckpointableKeyedStateBackend<Integer> keyedBackend =
            backend.createKeyedStateBackend(...)) {

        ValueStateDescriptor<String> descriptor =
            new ValueStateDescriptor<>("test", String.class);
        ValueState<String> state = keyedBackend.getState(descriptor);

        // Test state operations
        state.update("value");
        assertEquals("value", state.value());

        // Test checkpointing
        CheckpointStreamFactory streamFactory = ...;
        RunnableFuture<SnapshotResult<KeyedStateHandle>> snapshot =
            keyedBackend.snapshot(...);
        snapshot.run();

        // Verify snapshot
        SnapshotResult<KeyedStateHandle> result = snapshot.get();
        assertNotNull(result.getJobManagerOwnedSnapshot());
    }
}
```

### 2. Operator Tests with Test Harness
```java
@Test
public void testOperatorProcessing() throws Exception {
    MyOperator operator = new MyOperator();

    try (OneInputStreamOperatorTestHarness<String, String> harness =
            new OneInputStreamOperatorTestHarness<>(operator)) {

        harness.open();

        // Process elements
        harness.processElement("input1", 1000L);
        harness.processElement("input2", 2000L);

        // Process watermark
        harness.processWatermark(new Watermark(3000L));

        // Verify output
        ConcurrentLinkedQueue<Object> output = harness.getOutput();
        assertEquals(2, output.size());

        // Verify state
        assertEquals("expected", operator.getState());
    }
}
```

### 3. Async Operation Tests
```java
@Test
public void testAsyncOperation() throws Exception {
    CompletableFuture<String> future = new CompletableFuture<>();
    AsyncFunction function = new MyAsyncFunction();

    // Use countdown latch for synchronization
    CountDownLatch latch = new CountDownLatch(1);

    function.asyncInvoke("input", new ResultFuture<String>() {
        @Override
        public void complete(Collection<String> result) {
            future.complete(result.iterator().next());
            latch.countDown();
        }
    });

    // Wait with timeout
    assertTrue(latch.await(5, TimeUnit.SECONDS));
    assertEquals("expected", future.get());
}
```

### 4. Checkpoint and Recovery Tests
```java
@Test
public void testCheckpointRestore() throws Exception {
    // Create operator
    MyStatefulOperator operator = new MyStatefulOperator();

    // First run - process data and checkpoint
    try (OneInputStreamOperatorTestHarness<String, String> harness1 =
            new OneInputStreamOperatorTestHarness<>(operator)) {

        harness1.open();
        harness1.processElement("data1", 1000L);

        // Take snapshot
        OperatorSubtaskState snapshot = harness1.snapshot(1L, 2000L);

        // Verify pre-restore state
        assertEquals("state1", operator.getInternalState());
    }

    // Second run - restore from checkpoint
    MyStatefulOperator restoredOperator = new MyStatefulOperator();
    try (OneInputStreamOperatorTestHarness<String, String> harness2 =
            new OneInputStreamOperatorTestHarness<>(restoredOperator)) {

        // Restore from snapshot
        harness2.initializeState(snapshot);
        harness2.open();

        // Verify restored state
        assertEquals("state1", restoredOperator.getInternalState());

        // Continue processing
        harness2.processElement("data2", 3000L);
    }
}
```

### 5. Configuration Tests
```java
@Test
public void testConfiguration() {
    Configuration config = new Configuration();
    config.setString("key1", "value1");
    config.setInteger("key2", 42);

    MyConfigurableComponent component = new MyConfigurableComponent();
    component.configure(config);

    // Verify configuration applied
    assertEquals("value1", component.getConfigValue("key1"));
    assertEquals(42, component.getConfigValue("key2"));

    // Test default values
    assertNotNull(component.getConfigValue("defaultKey"));
}
```

## Mock and Stub Patterns

### Using Mockito
```java
@Test
public void testWithMocks() {
    // Create mocks
    CheckpointCoordinator mockCoordinator = mock(CheckpointCoordinator.class);
    ExecutionGraph mockGraph = mock(ExecutionGraph.class);

    // Setup behavior
    when(mockCoordinator.isShutdown()).thenReturn(false);
    when(mockGraph.getJobID()).thenReturn(new JobID());

    // Test with mocks
    MyComponent component = new MyComponent(mockCoordinator, mockGraph);
    component.process();

    // Verify interactions
    verify(mockCoordinator, times(1)).triggerCheckpoint();
    verify(mockGraph, never()).fail(any());
}
```

## Assertion Helpers

```java
// Custom assertions for Flink types
public static void assertStateEquals(ValueState<String> state, String expected)
        throws Exception {
    assertEquals(expected, state.value());
}

public static void assertWatermark(
        OneInputStreamOperatorTestHarness<?, ?> harness,
        long expected) {
    assertEquals(expected, harness.getCurrentWatermark());
}

// Verify exceptions
assertThrows(IllegalStateException.class, () -> {
    operator.processElement(null);
});
```

## Test Utilities

### Test Data Generators
```java
public static List<String> generateTestData(int count) {
    return IntStream.range(0, count)
        .mapToObj(i -> "test-" + i)
        .collect(Collectors.toList());
}
```

### Test Environment Setup
```java
@Before
public void setup() {
    Configuration config = new Configuration();
    config.setString(RestOptions.BIND_PORT, "0");

    cluster = new MiniClusterWithClientResource(
        new MiniClusterResourceConfiguration.Builder()
            .setConfiguration(config)
            .setNumberSlotsPerTaskManager(2)
            .setNumberTaskManagers(1)
            .build());

    cluster.before();
}

@After
public void teardown() {
    if (cluster != null) {
        cluster.after();
    }
}
```