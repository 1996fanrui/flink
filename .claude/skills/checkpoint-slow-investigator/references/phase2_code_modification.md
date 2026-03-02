# Phase 2: Code Modification Details

## Modification 1: Extend checkpoint timeout to 6 hours

Find the method that configures checkpoint settings (look for `CheckpointingOptions.CHECKPOINTING_TIMEOUT` or `setCheckpointTimeout` or the checkpoint configuration section). Add:
```java
conf.set(CheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofHours(6));
```

For `UnalignedCheckpointRescaleWithMixedExchangesITCase` specifically: in the `getUnalignedCheckpointEnv()` method, after the existing checkpoint configuration lines (around line 178), add the timeout setting.

## Modification 2: Fix REST port for API access

Find the cluster configuration section (MiniCluster config, look for `clusterConfig` or `Configuration` used for cluster setup). Add:
```java
clusterConfig.set(RestOptions.PORT, {REST_PORT});
```

For `UnalignedCheckpointRescaleWithMixedExchangesITCase` specifically: in the `setup()` method, after `clusterConfig` setup (around line 103), add the REST port setting.

## Modification 3: Add missing imports

Add these imports if not already present:
```java
import org.apache.flink.configuration.RestOptions;
```

(`CheckpointingOptions` and `Duration` should already be imported; if not, add them too.)
