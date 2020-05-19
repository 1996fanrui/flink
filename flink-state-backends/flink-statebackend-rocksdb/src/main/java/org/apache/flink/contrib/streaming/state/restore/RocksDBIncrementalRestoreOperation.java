/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.restore;

import org.apache.flink.contrib.streaming.state.RocksDBIncrementalCheckpointUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeySerializationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBKeyedStateBackend.RocksDbKvStateInfo;
import org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions;
import org.apache.flink.contrib.streaming.state.RocksDBOperationUtils;
import org.apache.flink.contrib.streaming.state.RocksDBStateDownloader;
import org.apache.flink.contrib.streaming.state.RocksDBWriteBatchWrapper;
import org.apache.flink.contrib.streaming.state.RocksIteratorWrapper;
import org.apache.flink.contrib.streaming.state.ttl.RocksDbTtlCompactFiltersManager;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.runtime.state.BackendBuildingException;
import org.apache.flink.runtime.state.DirectoryStateHandle;
import org.apache.flink.runtime.state.IncrementalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalLocalKeyedStateHandle;
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyedBackendSerializationProxy;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.RegisteredStateMetaInfoBase;
import org.apache.flink.runtime.state.StateHandleID;
import org.apache.flink.runtime.state.StateSerializerProvider;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.metainfo.StateMetaInfoSnapshot;
import org.apache.flink.util.IOUtils;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.UUID;
import java.util.function.Function;

import static org.apache.flink.contrib.streaming.state.snapshot.RocksSnapshotUtil.SST_FILE_SUFFIX;

/**
 * Encapsulates the process of restoring a RocksDB instance from an incremental snapshot.
 */
public class RocksDBIncrementalRestoreOperation<K> extends AbstractRocksDBRestoreOperation<K> {
	private static final Logger LOG = LoggerFactory.getLogger(RocksDBIncrementalRestoreOperation.class);

	private final String operatorIdentifier;
	// 要恢复的 sst 文件的集合，key 为 chk id，value 为 对应的 sst 文件集合
	private final SortedMap<Long, Set<StateHandleID>> restoredSstFiles;
	private long lastCompletedCheckpointId;
	private UUID backendUID;

	public RocksDBIncrementalRestoreOperation(
		String operatorIdentifier,
		KeyGroupRange keyGroupRange,
		int keyGroupPrefixBytes,
		int numberOfTransferringThreads,
		CloseableRegistry cancelStreamRegistry,
		ClassLoader userCodeClassLoader,
		Map<String, RocksDbKvStateInfo> kvStateInformation,
		StateSerializerProvider<K> keySerializerProvider,
		File instanceBasePath,
		File instanceRocksDBPath,
		DBOptions dbOptions,
		Function<String, ColumnFamilyOptions> columnFamilyOptionsFactory,
		RocksDBNativeMetricOptions nativeMetricOptions,
		MetricGroup metricGroup,
		@Nonnull Collection<KeyedStateHandle> restoreStateHandles,
		@Nonnull RocksDbTtlCompactFiltersManager ttlCompactFiltersManager) {
		super(keyGroupRange,
			keyGroupPrefixBytes,
			numberOfTransferringThreads,
			cancelStreamRegistry,
			userCodeClassLoader,
			kvStateInformation,
			keySerializerProvider,
			instanceBasePath,
			instanceRocksDBPath,
			dbOptions,
			columnFamilyOptionsFactory,
			nativeMetricOptions,
			metricGroup,
			restoreStateHandles,
			ttlCompactFiltersManager);
		this.operatorIdentifier = operatorIdentifier;
		this.restoredSstFiles = new TreeMap<>();
		this.lastCompletedCheckpointId = -1L;
		this.backendUID = UUID.randomUUID();
	}

	/**
	 * Root method that branches for different implementations of {@link KeyedStateHandle}.
	 */
	@Override
	public RocksDBRestoreResult restore() throws Exception {

		if (restoreStateHandles == null || restoreStateHandles.isEmpty()) {
			return null;
		}

		final KeyedStateHandle theFirstStateHandle = restoreStateHandles.iterator().next();

		// restoreStateHandles 数量大于 1，
		// 或者 恢复的 keyGroupRange 与当前负责的 keyGroupRange 不同，
		// 则使用 Rescaling 模式。如果没有改并发，则关闭 Rescaling 模式
		boolean isRescaling = (restoreStateHandles.size() > 1 ||
			!Objects.equals(theFirstStateHandle.getKeyGroupRange(), keyGroupRange));

		if (isRescaling) {
			// Rescaling 开启的恢复模式，相当于改并发恢复，需要依赖 KeyGroup 恢复
			//   - init from a certain sst: #createAndRegisterColumnFamilyDescriptors when prepare files, before db open
			//   - data ingestion after db open: #getOrRegisterStateColumnFamilyHandle before creating column family
			restoreWithRescaling(restoreStateHandles);
		} else {
			// Rescaling 关闭的恢复模式，相当于没有改变并发，直接恢复 sst 即可
			// 没有改并发就只有一个 StateHandle，所以这里只需要将 firstStateHandle 当做参数传递即可
			//   - restore without rescaling: #createAndRegisterColumnFamilyDescriptors when prepare files, before db open
			restoreWithoutRescaling(theFirstStateHandle);
		}
		return new RocksDBRestoreResult(this.db, defaultColumnFamilyHandle,
			nativeMetricMonitor, lastCompletedCheckpointId, backendUID, restoredSstFiles);
	}

	/**
	 * Recovery from a single remote incremental state without rescaling.
	 */
	private void restoreWithoutRescaling(KeyedStateHandle keyedStateHandle) throws Exception {
		if (keyedStateHandle instanceof IncrementalRemoteKeyedStateHandle) {
			// 远程的 KeyedStateHandle 恢复
			IncrementalRemoteKeyedStateHandle incrementalRemoteKeyedStateHandle =
				(IncrementalRemoteKeyedStateHandle) keyedStateHandle;

			// 恢复 State 之前, 整理要恢复的文件，即：要知道恢复哪些 文件
			restorePreviousIncrementalFilesStatus(incrementalRemoteKeyedStateHandle);

			/**
			 * 从远程恢复 sst 文件：
			 * 	1、 本地创建 tmp 文件
			 * 	2、 从远程拉取 sst 文件到本地，将 远程的 StateHandle 转换为 本地 的 StateHandle
			 * 	3、 调用 local 恢复 sst 文件的方法
			 * 	4、 清理 tmp 文件
 			 */
			restoreFromRemoteState(incrementalRemoteKeyedStateHandle);

		} else if (keyedStateHandle instanceof IncrementalLocalKeyedStateHandle) {
			// Local 的 KeyedStateHandle 恢复
			IncrementalLocalKeyedStateHandle incrementalLocalKeyedStateHandle =
				(IncrementalLocalKeyedStateHandle) keyedStateHandle;

			// 恢复 State 之前, 整理要恢复的文件，即：要知道恢复哪些文件
			restorePreviousIncrementalFilesStatus(incrementalLocalKeyedStateHandle);

			// 从 local 恢复 sst 文件的方法
			restoreFromLocalState(incrementalLocalKeyedStateHandle);
		} else {
			throw new BackendBuildingException("Unexpected state handle type, " +
				"expected " + IncrementalRemoteKeyedStateHandle.class + " or " + IncrementalLocalKeyedStateHandle.class +
				", but found " + keyedStateHandle.getClass());
		}
	}

	// 恢复 State 之前, 整理要恢复的文件
	// 将 StateHandle 中 chk id 与 sst 映射关系添加到 restoredSstFiles 中
	// 并将 StateHandle 的 Checkpoint id 保存到 lastCompletedCheckpointId 中
	private void restorePreviousIncrementalFilesStatus(IncrementalKeyedStateHandle localKeyedStateHandle) {
		backendUID = localKeyedStateHandle.getBackendIdentifier();
		restoredSstFiles.put(
			localKeyedStateHandle.getCheckpointId(),
			localKeyedStateHandle.getSharedStateHandleIDs());
		lastCompletedCheckpointId = localKeyedStateHandle.getCheckpointId();
	}

	private void restoreFromRemoteState(IncrementalRemoteKeyedStateHandle stateHandle) throws Exception {
		// 创建临时目录
		final Path tmpRestoreInstancePath = new Path(
			instanceBasePath.getAbsolutePath(),
			UUID.randomUUID().toString()); // used as restore source for IncrementalRemoteKeyedStateHandle
		try {
			// 从本地恢复
			restoreFromLocalState(
				// 从远程拉取 State 文件到本地，
				transferRemoteStateToLocalDirectory(tmpRestoreInstancePath, stateHandle));
		} finally {
			cleanUpPathQuietly(tmpRestoreInstancePath);
		}
	}

	// 从本地 State 文件中恢复 状态
	private void restoreFromLocalState(IncrementalLocalKeyedStateHandle localKeyedStateHandle) throws Exception {
		// 从 State 中获取元数据
		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(localKeyedStateHandle.getMetaDataState());
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		// 根据 State 的元数据，创建或注册 CF 描述符
		columnFamilyDescriptors = createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, true);
		columnFamilyHandles = new ArrayList<>(columnFamilyDescriptors.size() + 1);

		Path restoreSourcePath = localKeyedStateHandle.getDirectoryStateHandle().getDirectory();

		LOG.debug("Restoring keyed backend uid in operator {} from incremental snapshot to {}.",
			operatorIdentifier, backendUID);

		if (!instanceRocksDBPath.mkdirs()) {
			String errMsg = "Could not create RocksDB data directory: " + instanceBasePath.getAbsolutePath();
			LOG.error(errMsg);
			throw new IOException(errMsg);
		}

		// 准备数据目录
		restoreInstanceDirectoryFromPath(restoreSourcePath, dbPath);

		openDB();

		registerColumnFamilyHandles(stateMetaInfoSnapshots);
	}

	private IncrementalLocalKeyedStateHandle transferRemoteStateToLocalDirectory(
		Path temporaryRestoreInstancePath,
		IncrementalRemoteKeyedStateHandle restoreStateHandle) throws Exception {

		// try with resource 的方式创建 RocksDBStateDownloader
		try (RocksDBStateDownloader rocksDBStateDownloader = new RocksDBStateDownloader(numberOfTransferringThreads)) {

			// 具体的
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}

		// since we transferred all remote state to a local directory, we can use the same code as for
		// local recovery.
		return new IncrementalLocalKeyedStateHandle(
			restoreStateHandle.getBackendIdentifier(),
			restoreStateHandle.getCheckpointId(),
			new DirectoryStateHandle(temporaryRestoreInstancePath),
			restoreStateHandle.getKeyGroupRange(),
			restoreStateHandle.getMetaStateHandle(),
			restoreStateHandle.getSharedState().keySet());
	}

	private void cleanUpPathQuietly(@Nonnull Path path) {
		try {
			FileSystem fileSystem = path.getFileSystem();
			if (fileSystem.exists(path)) {
				fileSystem.delete(path, true);
			}
		} catch (IOException ex) {
			LOG.warn("Failed to clean up path " + path, ex);
		}
	}

	private void registerColumnFamilyHandles(List<StateMetaInfoSnapshot> metaInfoSnapshots) {
		// Register CF handlers
		for (int i = 0; i < metaInfoSnapshots.size(); ++i) {
			getOrRegisterStateColumnFamilyHandle(columnFamilyHandles.get(i), metaInfoSnapshots.get(i));
		}
	}

	/**
	 * rescaling 用于恢复多个 Incremental 的 State
	 * Recovery from multi incremental states with rescaling.
	 * For rescaling, this method creates a temporary RocksDB instance for a key-groups shard.
	 * All contents from the temporary instance are copied into the
	 * real restore instance and then the temporary instance is discarded.
	 *
	 *
	 */
	private void restoreWithRescaling(Collection<KeyedStateHandle> restoreStateHandles) throws Exception {

		// Prepare for restore with rescaling
		// 选取一个最好的 StateHandle 用于数据初始化，会有一个选择标准打分，分数最高，则被选中
		// 分数的计算规则：主要依赖 StateHandle 的 KeyGroup 与 当前 subtask 处理的 KeyGroup 求一个交集，看重复率
		KeyedStateHandle initialHandle = RocksDBIncrementalCheckpointUtils.chooseTheBestStateHandleForInitial(
			restoreStateHandles, keyGroupRange);

		// Init base DB instance
		if (initialHandle != null) {
			restoreStateHandles.remove(initialHandle);
			initDBWithRescaling(initialHandle);
		} else {
			openDB();
		}

		// Transfer remaining key-groups from temporary instance into base DB
		byte[] startKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getStartKeyGroup(), startKeyGroupPrefixBytes);

		byte[] stopKeyGroupPrefixBytes = new byte[keyGroupPrefixBytes];
		RocksDBKeySerializationUtils.serializeKeyGroup(keyGroupRange.getEndKeyGroup() + 1, stopKeyGroupPrefixBytes);

		for (KeyedStateHandle rawStateHandle : restoreStateHandles) {

			if (!(rawStateHandle instanceof IncrementalRemoteKeyedStateHandle)) {
				throw new IllegalStateException("Unexpected state handle type, " +
					"expected " + IncrementalRemoteKeyedStateHandle.class +
					", but found " + rawStateHandle.getClass());
			}

			Path temporaryRestoreInstancePath = new Path(instanceBasePath.getAbsolutePath() + UUID.randomUUID().toString());
			try (RestoredDBInstance tmpRestoreDBInfo = restoreDBInstanceFromStateHandle(
				(IncrementalRemoteKeyedStateHandle) rawStateHandle,
				temporaryRestoreInstancePath);
				RocksDBWriteBatchWrapper writeBatchWrapper = new RocksDBWriteBatchWrapper(this.db)) {

				List<ColumnFamilyDescriptor> tmpColumnFamilyDescriptors = tmpRestoreDBInfo.columnFamilyDescriptors;
				List<ColumnFamilyHandle> tmpColumnFamilyHandles = tmpRestoreDBInfo.columnFamilyHandles;

				// iterating only the requested descriptors automatically skips the default column family handle
				for (int i = 0; i < tmpColumnFamilyDescriptors.size(); ++i) {
					ColumnFamilyHandle tmpColumnFamilyHandle = tmpColumnFamilyHandles.get(i);

					ColumnFamilyHandle targetColumnFamilyHandle = getOrRegisterStateColumnFamilyHandle(
						null, tmpRestoreDBInfo.stateMetaInfoSnapshots.get(i))
						.columnFamilyHandle;

					try (RocksIteratorWrapper iterator = RocksDBOperationUtils.getRocksIterator(tmpRestoreDBInfo.db, tmpColumnFamilyHandle)) {

						iterator.seek(startKeyGroupPrefixBytes);

						while (iterator.isValid()) {

							if (RocksDBIncrementalCheckpointUtils.beforeThePrefixBytes(iterator.key(), stopKeyGroupPrefixBytes)) {
								writeBatchWrapper.put(targetColumnFamilyHandle, iterator.key(), iterator.value());
							} else {
								// Since the iterator will visit the record according to the sorted order,
								// we can just break here.
								break;
							}

							iterator.next();
						}
					} // releases native iterator resources
				}
			} finally {
				cleanUpPathQuietly(temporaryRestoreInstancePath);
			}
		}
	}

	private void initDBWithRescaling(KeyedStateHandle initialHandle) throws Exception {

		assert (initialHandle instanceof IncrementalRemoteKeyedStateHandle);

		// 1. Restore base DB from selected initial handle
		restoreFromRemoteState((IncrementalRemoteKeyedStateHandle) initialHandle);

		// 2. Clip the base DB instance
		try {
			RocksDBIncrementalCheckpointUtils.clipDBWithKeyGroupRange(
				db,
				columnFamilyHandles,
				keyGroupRange,
				initialHandle.getKeyGroupRange(),
				keyGroupPrefixBytes);
		} catch (RocksDBException e) {
			String errMsg = "Failed to clip DB after initialization.";
			LOG.error(errMsg, e);
			throw new BackendBuildingException(errMsg, e);
		}
	}

	/**
	 * Entity to hold the temporary RocksDB instance created for restore.
	 */
	private static class RestoredDBInstance implements AutoCloseable {

		@Nonnull
		private final RocksDB db;

		@Nonnull
		private final ColumnFamilyHandle defaultColumnFamilyHandle;

		@Nonnull
		private final List<ColumnFamilyHandle> columnFamilyHandles;

		@Nonnull
		private final List<ColumnFamilyDescriptor> columnFamilyDescriptors;

		@Nonnull
		private final List<StateMetaInfoSnapshot> stateMetaInfoSnapshots;

		private RestoredDBInstance(
			@Nonnull RocksDB db,
			@Nonnull List<ColumnFamilyHandle> columnFamilyHandles,
			@Nonnull List<ColumnFamilyDescriptor> columnFamilyDescriptors,
			@Nonnull List<StateMetaInfoSnapshot> stateMetaInfoSnapshots) {
			this.db = db;
			this.defaultColumnFamilyHandle = columnFamilyHandles.remove(0);
			this.columnFamilyHandles = columnFamilyHandles;
			this.columnFamilyDescriptors = columnFamilyDescriptors;
			this.stateMetaInfoSnapshots = stateMetaInfoSnapshots;
		}

		@Override
		public void close() {
			List<ColumnFamilyOptions> columnFamilyOptions = new ArrayList<>(columnFamilyDescriptors.size() + 1);
			columnFamilyDescriptors.forEach((cfd) -> columnFamilyOptions.add(cfd.getOptions()));
			RocksDBOperationUtils.addColumnFamilyOptionsToCloseLater(columnFamilyOptions, defaultColumnFamilyHandle);
			IOUtils.closeQuietly(defaultColumnFamilyHandle);
			IOUtils.closeAllQuietly(columnFamilyHandles);
			IOUtils.closeQuietly(db);
			IOUtils.closeAllQuietly(columnFamilyOptions);
		}
	}

	private RestoredDBInstance restoreDBInstanceFromStateHandle(
		IncrementalRemoteKeyedStateHandle restoreStateHandle,
		Path temporaryRestoreInstancePath) throws Exception {

		try (RocksDBStateDownloader rocksDBStateDownloader =
				new RocksDBStateDownloader(numberOfTransferringThreads)) {
			rocksDBStateDownloader.transferAllStateDataToDirectory(
				restoreStateHandle,
				temporaryRestoreInstancePath,
				cancelStreamRegistry);
		}

		KeyedBackendSerializationProxy<K> serializationProxy = readMetaData(restoreStateHandle.getMetaStateHandle());
		// read meta data
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots = serializationProxy.getStateMetaInfoSnapshots();

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			createAndRegisterColumnFamilyDescriptors(stateMetaInfoSnapshots, false);

		List<ColumnFamilyHandle> columnFamilyHandles =
			new ArrayList<>(stateMetaInfoSnapshots.size() + 1);

		RocksDB restoreDb = RocksDBOperationUtils.openDB(
			temporaryRestoreInstancePath.getPath(),
			columnFamilyDescriptors,
			columnFamilyHandles,
			RocksDBOperationUtils.createColumnFamilyOptions(columnFamilyOptionsFactory, "default"),
			dbOptions);

		return new RestoredDBInstance(restoreDb, columnFamilyHandles, columnFamilyDescriptors, stateMetaInfoSnapshots);
	}

	/**
	 * This method recreates and registers all {@link ColumnFamilyDescriptor} from Flink's state meta data snapshot.
	 *
	 */
	private List<ColumnFamilyDescriptor> createAndRegisterColumnFamilyDescriptors(
		List<StateMetaInfoSnapshot> stateMetaInfoSnapshots,
		boolean registerTtlCompactFilter) {

		List<ColumnFamilyDescriptor> columnFamilyDescriptors =
			new ArrayList<>(stateMetaInfoSnapshots.size());

		for (StateMetaInfoSnapshot stateMetaInfoSnapshot : stateMetaInfoSnapshots) {
			// 根据 元数据信息 创建 columnFamilyDescriptors
			RegisteredStateMetaInfoBase metaInfoBase =
				RegisteredStateMetaInfoBase.fromMetaInfoSnapshot(stateMetaInfoSnapshot);

			ColumnFamilyDescriptor columnFamilyDescriptor = RocksDBOperationUtils.createColumnFamilyDescriptor(
				metaInfoBase, columnFamilyOptionsFactory, registerTtlCompactFilter ? ttlCompactFiltersManager : null);

			columnFamilyDescriptors.add(columnFamilyDescriptor);
		}
		return columnFamilyDescriptors;
	}

	/**
	 * This recreates the new working directory of the recovered RocksDB instance and links/copies the contents from
	 * a local state.
	 * 创建 RocksDB 的目录，文件名后缀是 .sst，则创建 link 即可，其他文件要真实拷贝到 db 目录
	 */
	private void restoreInstanceDirectoryFromPath(Path source, String instanceRocksDBPath) throws IOException {

		FileSystem fileSystem = source.getFileSystem();

		final FileStatus[] fileStatuses = fileSystem.listStatus(source);

		if (fileStatuses == null) {
			throw new IOException("Cannot list file statues. Directory " + source + " does not exist.");
		}

		for (FileStatus fileStatus : fileStatuses) {
			final Path filePath = fileStatus.getPath();
			final String fileName = filePath.getName();
			File restoreFile = new File(source.getPath(), fileName);
			File targetFile = new File(instanceRocksDBPath, fileName);
			if (fileName.endsWith(SST_FILE_SUFFIX)) {
				// hardlink'ing the immutable sst-files.
				Files.createLink(targetFile.toPath(), restoreFile.toPath());
			} else {
				// true copy for all other files.
				Files.copy(restoreFile.toPath(), targetFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}

	/**
	 * Reads Flink's state meta data file from the state handle.
	 */
	private KeyedBackendSerializationProxy<K> readMetaData(StreamStateHandle metaStateHandle) throws Exception {

		FSDataInputStream inputStream = null;

		try {
			inputStream = metaStateHandle.openInputStream();
			cancelStreamRegistry.registerCloseable(inputStream);
			DataInputView in = new DataInputViewStreamWrapper(inputStream);
			return readMetaData(in);
		} finally {
			if (cancelStreamRegistry.unregisterCloseable(inputStream)) {
				inputStream.close();
			}
		}
	}
}
