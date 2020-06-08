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

package org.apache.flink.runtime.state.heap;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateTransformationFunction;
import org.apache.flink.runtime.state.internal.InternalKvState;
import org.apache.flink.util.MathUtils;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.util.*;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.apache.flink.util.CollectionUtil.MAX_ARRAY_SIZE;

/**
 * Implementation of Flink's in-memory state maps with copy-on-write support. This map does not support null values
 * for key or namespace.
 *
 * <p>{@link CopyOnWriteStateMap} sacrifices some peak performance and memory efficiency for features like incremental
 * rehashing and asynchronous snapshots through copy-on-write. Copy-on-write tries to minimize the amount of copying by
 * maintaining version meta data for both, the map structure and the state objects. However, we must often proactively
 * copy state objects when we hand them to the user.
 *
 * <p>As for any state backend, user should not keep references on state objects that they obtained from state backends
 * outside the scope of the user function calls.
 *
 * <p>Some brief maintenance notes:
 *
 * <p>1) Flattening the underlying data structure from nested maps (namespace) -> (key) -> (state) to one flat map
 * (key, namespace) -> (state) brings certain performance trade-offs. In theory, the flat map has one less level of
 * indirection compared to the nested map. However, the nested map naturally de-duplicates namespace objects for which
 * #equals() is true. This leads to potentially a lot of redundant namespace objects for the flattened version. Those,
 * in turn, can again introduce more cache misses because we need to follow the namespace object on all operations to
 * ensure entry identities. Obviously, copy-on-write can also add memory overhead. So does the meta data to track
 * copy-on-write requirement (state and entry versions on {@link StateMapEntry}).
 * 嵌套 map 和 拍平的 map 的对比：
 * 1、 嵌套 map：Map<namespace, Map<key, state>>
 *     拍平的 map： Map<(key, namespace) -> (state)> 。实际上是 一个 Entry 里包含了  key, namespace，state 三个数据字段
*  2、 拍平的 map 少了一层嵌套，但是每条数据都需要存储 namespace
 *  而 嵌套模式下， namespace 只需要存储一遍
 *
 *  3、 cow 和 StateMapEntry 的 元数据 都需要消耗内存空间
 *
 * <p>2) A flat map structure is a lot easier when it comes to tracking copy-on-write of the map structure.
 * 			flat map 结构更容易实现 cow
 *
 * <p>3) Nested structure had the (never used) advantage that we can easily drop and iterate whole namespaces. This could
 * give locality advantages for certain access pattern, e.g. iterating a namespace.
 * 		嵌套结果更容易删除和迭代 整个 namespace。迭代 某一个 namespace 时，嵌套结构是有优势的
 *
 * <p>4) Serialization format is changed from namespace-prefix compressed (as naturally provided from the old nested
 * structure) to making all entries self contained as (key, namespace, state).
 *
 * <p>5) Currently, a state map can only grow, but never shrinks on low load. We could easily add this if required.
 * 		目前 map 只支持扩容，不支持缩容
 *
 * <p>6) Heap based state backends like this can easily cause a lot of GC activity. Besides using G1 as garbage collector,
 * we should provide an additional state backend that operates on off-heap memory. This would sacrifice peak performance
 * (due to de/serialization of objects) for a lower, but more constant throughput and potentially huge simplifications
 * w.r.t. copy-on-write.
 * 	基于 Heap 模式的，容易发生 GC。如果不使用 G1，应该提供 off-heap memory 的模式。
 * 	但是堆外内存的模式增加了序列化和反序列的过程，所以降低了 峰值的性能。
 * 	但是 off-heap 更容易实现 cow
 *
 * <p>7) We could try a hybrid of a serialized and object based backends, where key and namespace of the entries are both
 * serialized in one byte-array.
 *
 *
 * <p>9) We could consider smaller types (e.g. short) for the version counting and think about some reset strategy before
 * overflows, when there is no snapshot running. However, this would have to touch all entries in the map.
 *  可以考虑用更节省空间的 short 类型来保存 version。
 *  当没有 snapshot 在运行时，可以考虑一些 reset 策略。所谓的 reset 策略就是遍历所有的 entry，将其 version 置为 0
 *
 * <p>This class was initially based on the {@link java.util.HashMap} implementation of the Android JDK, but is now heavily
 * customized towards the use case of map for state entries.
 * IMPORTANT: the contracts for this class rely on the user not holding any references to objects returned by this map
 * beyond the life cycle of per-element operations. Or phrased differently, all get-update-put operations on a mapping
 * should be within one call of processElement. Otherwise, the user must take care of taking deep copies, e.g. for
 * caching purposes.
 *
 * @param <K> type of key.
 * @param <N> type of namespace.
 * @param <S> type of value.
 */
public class CopyOnWriteStateMap<K, N, S> extends StateMap<K, N, S> {

	/**
	 * The logger.
	 */
	private static final Logger LOG = LoggerFactory.getLogger(HeapKeyedStateBackend.class);

	/**
	 * Min capacity (other than zero) for a {@link CopyOnWriteStateMap}. Must be a power of two
	 * greater than 1 (and less than 1 << 30).
	 */
	private static final int MINIMUM_CAPACITY = 4;

	/**
	 * Max capacity for a {@link CopyOnWriteStateMap}. Must be a power of two >= MINIMUM_CAPACITY.
	 */
	private static final int MAXIMUM_CAPACITY = 1 << 30;

	/**
	 * Default capacity for a {@link CopyOnWriteStateMap}. Must be a power of two,
	 * greater than {@code MINIMUM_CAPACITY} and less than {@code MAXIMUM_CAPACITY}.
	 * 默认容量 128，即： hash 表中桶的个数默认 128
	 */
	public static final int DEFAULT_CAPACITY = 128;

	/**
	 * Minimum number of entries that one step of incremental rehashing migrates from the old to the new sub-map.
	 * hash 扩容迁移数据时，每次最少要迁移 4 条数据
	 */
	private static final int MIN_TRANSFERRED_PER_INCREMENTAL_REHASH = 4;

	/**
	 * The serializer of the state.
	 * State 的序列化器
	 */
	protected final TypeSerializer<S> stateSerializer;

	/**
	 * An empty map shared by all zero-capacity maps (typically from default
	 * constructor). It is never written to, and replaced on first put. Its size
	 * is set to half the minimum, so that the first resize will create a
	 * minimum-sized map.
	 * 空表：提前创建好
	 */
	private static final StateMapEntry<?, ?, ?>[] EMPTY_TABLE = new StateMapEntry[MINIMUM_CAPACITY >>> 1];

	/**
	 * Empty entry that we use to bootstrap our {@link CopyOnWriteStateMap.StateEntryIterator}.
	 */
	private static final StateMapEntry<?, ?, ?> ITERATOR_BOOTSTRAP_ENTRY =
		new StateMapEntry<>(new Object(), new Object(), new Object(), 0, null, 0, 0);

	/**
	 * Maintains an ordered set of version ids that are still in use by unreleased snapshots.
	 * 所有 正在进行中的 snapshot 的 version。
	 * 每次创建出一个 Snapshot 时，都需要将 Snapshot 的 version 保存到该 Set 中
	 */
	private final TreeSet<Integer> snapshotVersions;

	/**
	 * This is the primary entry array (hash directory) of the state map. If no incremental rehash is ongoing, this
	 * is the only used table.
	 * 主表：用于存储数据的 table
	 **/
	private StateMapEntry<K, N, S>[] primaryTable;

	/**
	 * We maintain a secondary entry array while performing an incremental rehash. The purpose is to slowly migrate
	 * entries from the primary table to this resized table array. When all entries are migrated, this becomes the new
	 * primary table.
	 * 扩容时的新表，扩容期间数组长度为 primaryTable 的 2 倍。
	 * 非扩容期间为 空表
	 */
	private StateMapEntry<K, N, S>[] incrementalRehashTable;

	/**
	 * The current number of mappings in the primary talbe.
	 * primaryTable 中元素个数
	 */
	private int primaryTableSize;

	/**
	 * The current number of mappings in the rehash table.
	 * incrementalRehashTable 中元素个数
	 */
	private int incrementalRehashTableSize;

	/**
	 * The next index for a step of incremental rehashing in the primary table.
	 * primary table 中增量 rehash 要迁移的下一个 index
	 * 即：primaryTable 中 rehashIndex 之前的数据全部搬移完成
	 */
	private int rehashIndex;

	/**
	 * The current version of this map. Used for copy-on-write mechanics.
	 * 当前 StateMap 的 version，每次创建一个 Snapshot 时，StateMap 的版本号加一
	 */
	private int stateMapVersion;

	/**
	 * The highest version of this map that is still required by any unreleased snapshot.
	 * 正在进行中的那些 snapshot 的最大版本号
	 * 这里保存的就是 TreeSet<Integer> snapshotVersions 中最大的版本号
	 */
	private int highestRequiredSnapshotVersion;

	/**
	 * The last namespace that was actually inserted. This is a small optimization to reduce duplicate namespace objects.
	 */
	private N lastNamespace;

	/**
	 * The {@link CopyOnWriteStateMap} is rehashed when its size exceeds this threshold.
	 * The value of this field is generally .75 * capacity, except when
	 * the capacity is zero, as described in the EMPTY_TABLE declaration
	 * above.
	 * 扩容阈值，与 HashMap 类似，当元素个数大于 threshold 时，就会开始扩容。
	 * 默认 threshold 为 StateMap 容量 * 0.75
	 */
	private int threshold;

	/**
	 * Incremented by "structural modifications" to allow (best effort)
	 * detection of concurrent modification.
	 * 用于记录元素修改的次数，遍历迭代过程中，发现 modCount 修改了，则抛异常
	 */
	private int modCount;

	/**
	 * Constructs a new {@code StateMap} with default capacity of {@code DEFAULT_CAPACITY}.
	 *
	 * @param stateSerializer the serializer of the key.
	 */
	CopyOnWriteStateMap(TypeSerializer<S> stateSerializer) {
		this(DEFAULT_CAPACITY, stateSerializer);
	}

	/**
	 * Constructs a new {@code StateMap} instance with the specified capacity.
	 *
	 * @param capacity      the initial capacity of this hash map.
	 * @param stateSerializer the serializer of the key.
	 * @throws IllegalArgumentException when the capacity is less than zero.
	 */
	@SuppressWarnings("unchecked")
	private CopyOnWriteStateMap(
		int capacity, TypeSerializer<S> stateSerializer) {
		this.stateSerializer = Preconditions.checkNotNull(stateSerializer);

		// initialized maps to EMPTY_TABLE.
		this.primaryTable = (StateMapEntry<K, N, S>[]) EMPTY_TABLE;
		this.incrementalRehashTable = (StateMapEntry<K, N, S>[]) EMPTY_TABLE;

		// initialize sizes to 0.
		this.primaryTableSize = 0;
		this.incrementalRehashTableSize = 0;

		this.rehashIndex = 0;
		this.stateMapVersion = 0;
		this.highestRequiredSnapshotVersion = 0;
		this.snapshotVersions = new TreeSet<>();

		if (capacity < 0) {
			throw new IllegalArgumentException("Capacity: " + capacity);
		}

		if (capacity == 0) {
			threshold = -1;
			return;
		}

		if (capacity < MINIMUM_CAPACITY) {
			capacity = MINIMUM_CAPACITY;
		} else if (capacity > MAXIMUM_CAPACITY) {
			capacity = MAXIMUM_CAPACITY;
		} else {
			capacity = MathUtils.roundUpToPowerOfTwo(capacity);
		}
		primaryTable = makeTable(capacity);
	}

	// Public API from StateMap ------------------------------------------------------------------------------

	/**
	 * Returns the total number of entries in this {@link CopyOnWriteStateMap}. This is the sum of both sub-maps.
	 *
	 * @return the number of entries in this {@link CopyOnWriteStateMap}.
	 */
	@Override
	public int size() {
		return primaryTableSize + incrementalRehashTableSize;
	}

	@Override
	public S get(K key, N namespace) {

		// 迁移数据，并且计算 hash 值
		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		//
		final int requiredVersion = highestRequiredSnapshotVersion;
		// rehashIndex 之前的桶全部 rehash 结束，
		// 所以按照旧桶的 hash 策略，如果 hash 分桶 >= rehashIndex，返回 primary 表，否则返回 Increment 表
		final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			final K eKey = e.key;
			final N eNamespace = e.namespace;
			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {

				// 一旦 get 当前数据，为了防止应用层修改数据内部的属性值，
				// 所以必须保证这是一个最新的 Entry，并更新其 stateVersion

				// 首先检查当前的 State，也就是 value 值是否是旧版本数据，
				// 如果 value 是旧版本，则必须深拷贝一个 value
				// 否则 value 是新版本，直接返回给应用层
				// copy-on-write check for state
				if (e.stateVersion < requiredVersion) {
					// copy-on-write check for entry
					// 此时还有两种情况，
					// 1、如果当前 Entry 是旧版本的，则 Entry 也需要拷贝一份，
					// 		按照之前分析过的 handleChainedEntryCopyOnWrite 策略拷贝即可
					// 2、当前 Entry 是新版本数据，则不需要拷贝，直接修改其 State 即可
					if (e.entryVersion < requiredVersion) {
						e = handleChainedEntryCopyOnWrite(tab, hash & (tab.length - 1), e);
					}
					// 更新其 stateVersion
					e.stateVersion = stateMapVersion;
					// 通过序列化器，深拷贝一个数据
					e.state = getStateSerializer().copy(e.state);
				}

				return e.state;
			}
		}

		// 没有找到相关的 Entry，所以返回 null
		return null;
	}

	@Override
	public boolean containsKey(K key, N namespace) {
		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			final K eKey = e.key;
			final N eNamespace = e.namespace;

			if ((e.hash == hash && key.equals(eKey) && namespace.equals(eNamespace))) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void put(K key, N namespace, S value) {
		// putEntry 用于找到对应的 Entry，
		// 包括了修改数据或插入新数据的场景
		final StateMapEntry<K, N, S> e = putEntry(key, namespace);

		// 将 value set 到 Entry 中
		e.state = value;
		// state 更新了，所以要更新 stateVersion
		e.stateVersion = stateMapVersion;
	}

	@Override
	public S putAndGetOld(K key, N namespace, S state) {
		final StateMapEntry<K, N, S> e = putEntry(key, namespace);

		// copy-on-write check for state
		S oldState = (e.stateVersion < highestRequiredSnapshotVersion) ?
			getStateSerializer().copy(e.state) :
			e.state;

		e.state = state;
		e.stateVersion = stateMapVersion;

		return oldState;
	}

	@Override
	public void remove(K key, N namespace) {
		removeEntry(key, namespace);
	}

	@Override
	public S removeAndGetOld(K key, N namespace) {

		final StateMapEntry<K, N, S> e = removeEntry(key, namespace);

		return e != null ?
			// copy-on-write check for state
			(e.stateVersion < highestRequiredSnapshotVersion ?
				getStateSerializer().copy(e.state) :
				e.state) :
			null;
	}

	@Override
	public Stream<K> getKeys(N namespace) {
		return StreamSupport.stream(spliterator(), false)
			.filter(entry -> entry.getNamespace().equals(namespace))
			.map(StateEntry::getKey);
	}

	@Override
	public <T> void transform(
		K key,
		N namespace,
		T value,
		StateTransformationFunction<S, T> transformation) throws Exception {

		final StateMapEntry<K, N, S> entry = putEntry(key, namespace);

		// copy-on-write check for state
		entry.state = transformation.apply(
			(entry.stateVersion < highestRequiredSnapshotVersion) ?
				getStateSerializer().copy(entry.state) :
				entry.state,
			value);
		entry.stateVersion = stateMapVersion;
	}

	// Private implementation details of the API methods ---------------------------------------------------------------

	/**
	 * Helper method that is the basis for operations that add mappings.
	 * 找到 key、namespace 应该要修改的 Entry，但不 set 属性
	 * 如果当前 key、namespace 对应 Entry 的 version 大于 highestRequiredSnapshotVersion 直接返回该 Entry
	 *
	 */
	private StateMapEntry<K, N, S> putEntry(K key, N namespace) {

		// 计算当前对应的 hash 值，选择 primaryTable 或 incrementalRehashTable
		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		// 遍历当前桶中链表的一个个 Entry
		for (StateMapEntry<K, N, S> e = tab[index]; e != null; e = e.next) {
			// 如果根据 key 和 namespace 找到了对应的 Entry，则认为是修改数据
			// 普通的 HashMap 结构有一个 Key ，而这里 key 和 namespace 的组合当做 key
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {

				// copy-on-write check for entry
				// entryVersion 表示 entry 创建时的版本号
				// highestRequiredSnapshotVersion 表示 正在进行中的那些 snapshot 的最大版本号
				// entryVersion 小于 highestRequiredSnapshotVersion，说明 Entry 的版本小于当前某些 Snapshot 的版本号，
				// 即：当前 Entry 是旧版本的数据，当前 Entry 被其他 snapshot 持有。
				// 为了保证 Snapshot 的数据正确性，这里必须为 e 创建新的副本，且 e 之前的某些元素也需要 copy 副本
				// 然后将返回 handleChainedEntryCopyOnWrite 方法返回的 e 的副本返回给上层，进行数据的修改操作。
				if (e.entryVersion < highestRequiredSnapshotVersion) {
					e = handleChainedEntryCopyOnWrite(tab, index, e);
				}

				// 反之，entryVersion >= highestRequiredSnapshotVersion
				// 说明当前 Entry 创建时的 version 比所有 Snapshot 的 version 要大
				// 即：当前 Entry 是新版本的数据，不被任何 Snapshot 持有
				// 注：Snapshot 不可能引用高版本的数据
				// 此时，e 是新的 Entry，不存在共享问题，所以直接修改当前 Entry 即可，所以返回当前 e
				return e;
			}
		}

		// 代码走到这里，说明原始的链表中没找到对应 Entry，即：插入新数据的逻辑
		++modCount;
		if (size() > threshold) {
			doubleCapacity();
		}

		// 链中没有找到 key 和 namespace 的数据
		return addNewStateMapEntry(tab, key, namespace, hash);
	}

	/**
	 * Helper method that is the basis for operations that remove mappings.
	 */
	private StateMapEntry<K, N, S> removeEntry(K key, N namespace) {

		final int hash = computeHashForOperationAndDoIncrementalRehash(key, namespace);
		final StateMapEntry<K, N, S>[] tab = selectActiveTable(hash);
		int index = hash & (tab.length - 1);

		for (StateMapEntry<K, N, S> e = tab[index], prev = null; e != null; prev = e, e = e.next) {
			if (e.hash == hash && key.equals(e.key) && namespace.equals(e.namespace)) {
				// 如果要删除的 Entry 不存在前继节点，说明要删除的 Entry 是头结点，
				// 直接将桶直接指向头结点的 next 节点即可。
				if (prev == null) {
					tab[index] = e.next;
				} else {
					// 如果 remove 的 Entry 不是链表头节点，需要将目标 Entry 之前的所有 Entry 拷贝一份，
					// 且目标 Entry 前一个节点的副本直接指向目标 Entry 的下一个节点。
					// 当然如果前继节点已经是新版本了，则不需要拷贝，直接修改前继 Entry 的 next 指针即可。
					// copy-on-write check for entry
					if (prev.entryVersion < highestRequiredSnapshotVersion) {
						prev = handleChainedEntryCopyOnWrite(tab, index, prev);
					}
					prev.next = e.next;
				}
				// 修改一些计数器
				++modCount;
				if (tab == primaryTable) {
					--primaryTableSize;
				} else {
					--incrementalRehashTableSize;
				}
				return e;
			}
		}
		return null;
	}

	// Iteration  ------------------------------------------------------------------------------------------------------

	@Nonnull
	@Override
	public Iterator<StateEntry<K, N, S>> iterator() {
		return new StateEntryIterator();
	}

	// Private utility functions for StateMap management -------------------------------------------------------------

	/**
	 * @see #releaseSnapshot(StateMapSnapshot)
	 * releaseSnapshot 只是更新 highestRequiredSnapshotVersion，并不删除那些旧版本的数据
	 */
	@VisibleForTesting
	void releaseSnapshot(int snapshotVersion) {
		// we guard against concurrent modifications of highestRequiredSnapshotVersion between snapshot and release.
		// Only stale reads of from the result of #releaseSnapshot calls are ok.
		synchronized (snapshotVersions) {
			// 将 相应的 snapshotVersion 从 snapshotVersions 中 remove
			Preconditions.checkState(snapshotVersions.remove(snapshotVersion), "Attempt to release unknown snapshot version");
			// 将 snapshotVersions 的最大值更新到 highestRequiredSnapshotVersion，
			// 如果snapshotVersions 为空，则 highestRequiredSnapshotVersion 更新为 0
			highestRequiredSnapshotVersion = snapshotVersions.isEmpty() ? 0 : snapshotVersions.last();
		}
	}

	/**
	 * Creates (combined) copy of the table arrays for a snapshot. This method must be called by the same Thread that
	 * does modifications to the {@link CopyOnWriteStateMap}.
	 * stateSnapshot 方法创建 CopyOnWriteStateMapSnapshot 时会调用该方法，
	 * 用于对 Map 数据创建一个 副本保存到 Snapshot 中
	 */
	@VisibleForTesting
	@SuppressWarnings("unchecked")
	StateMapEntry<K, N, S>[] snapshotMapArrays() {


		// 1、stateMapVersion 版本 + 1，赋值给 highestRequiredSnapshotVersion，
		// 并加入snapshotVersions
		// 这个时候，如果有新的StateTableEntry操作，如果StateTableEntry的版本小于 highestRequiredSnapshotVersion，
		// highestRequiredSnapshotVersion 之前的数据还要保存旧版本，所以不能修改
		// 则会拷贝StateTableEntry进行操作，从而达到copy-on-write的效果。

		// we guard against concurrent modifications of highestRequiredSnapshotVersion between snapshot and release.
		// Only stale reads of from the result of #releaseSnapshot calls are ok. This is why we must call this method
		// from the same thread that does all the modifications to the map.
		synchronized (snapshotVersions) {

			// increase the map version for copy-on-write and register the snapshot
			// stateMapVersion 越界了，抛异常挂掉
			if (++stateMapVersion < 0) {
				// this is just a safety net against overflows, but should never happen in practice (i.e., only after 2^31 snapshots)
				throw new IllegalStateException("Version count overflow in CopyOnWriteStateMap. Enforcing restart.");
			}

			highestRequiredSnapshotVersion = stateMapVersion;
			snapshotVersions.add(highestRequiredSnapshotVersion);
		}

		// 2、 将现在 primary 和 Increment 的元素浅拷贝一份到 copy 中
		// copy 策略：copy 数组长度为 primary 中剩余的桶数 + Increment 中有数据的桶数
		// primary 中剩余的数据放在 copy 数组的前面，Increment 中低位数据随后，
		// Increment 中高位数据放到 copy 数组的最后
		StateMapEntry<K, N, S>[] table = primaryTable;

		// In order to reuse the copied array as the destination array for the partitioned records in
		// CopyOnWriteStateMapSnapshot.TransformedSnapshotIterator, we need to make sure that the copied array
		// is big enough to hold the flattened entries. In fact, given the current rehashing algorithm, we only
		// need to do this check when isRehashing() is false, but in order to get a more robust code(in case that
		// the rehashing algorithm may changed in the future), we do this check for all the case.
		final int totalMapIndexSize = rehashIndex + table.length;
		final int copiedArraySize = Math.max(totalMapIndexSize, size());
		final StateMapEntry<K, N, S>[] copy = new StateMapEntry[copiedArraySize];

		if (isRehashing()) {
			// consider both maps for the snapshot, the rehash index tells us which part of the two maps we need
			final int localRehashIndex = rehashIndex;
			final int localCopyLength = table.length - localRehashIndex;
			// for the primary table, take every index >= rhIdx.
			System.arraycopy(table, localRehashIndex, copy, 0, localCopyLength);

			// for the new table, we are sure that two regions contain all the entries:
			// [0, rhIdx[ AND [table.length / 2, table.length / 2 + rhIdx[
			table = incrementalRehashTable;
			System.arraycopy(table, 0, copy, localCopyLength, localRehashIndex);
			System.arraycopy(table, table.length >>> 1, copy, localCopyLength + localRehashIndex, localRehashIndex);
		} else {
			// we only need to copy the primary table
			System.arraycopy(table, 0, copy, 0, table.length);
		}

		return copy;
	}

	int getStateMapVersion() {
		return stateMapVersion;
	}

	/**
	 * Allocate a table of the given capacity and set the threshold accordingly.
	 *
	 * @param newCapacity must be a power of two
	 */
	private StateMapEntry<K, N, S>[] makeTable(int newCapacity) {

		if (newCapacity < MAXIMUM_CAPACITY) {
			threshold = (newCapacity >> 1) + (newCapacity >> 2); // 3/4 capacity
		} else {
			if (size() > MAX_ARRAY_SIZE) {

				throw new IllegalStateException("Maximum capacity of CopyOnWriteStateMap is reached and the job " +
					"cannot continue. Please consider scaling-out your job or using a different keyed state backend " +
					"implementation!");
			} else {

				LOG.warn("Maximum capacity of 2^30 in StateMap reached. Cannot increase hash map size. This can " +
					"lead to more collisions and lower performance. Please consider scaling-out your job or using a " +
					"different keyed state backend implementation!");
				threshold = MAX_ARRAY_SIZE;
			}
		}

		@SuppressWarnings("unchecked") StateMapEntry<K, N, S>[] newMap =
			(StateMapEntry<K, N, S>[]) new StateMapEntry[newCapacity];
		return newMap;
	}

	/**
	 * Creates and inserts a new {@link StateMapEntry}.
	 */
	private StateMapEntry<K, N, S> addNewStateMapEntry(
		StateMapEntry<K, N, S>[] table,
		K key,
		N namespace,
		int hash) {

		// small optimization that aims to avoid holding references on duplicate namespace objects
		if (namespace.equals(lastNamespace)) {
			namespace = lastNamespace;
		} else {
			lastNamespace = namespace;
		}

		int index = hash & (table.length - 1);
		StateMapEntry<K, N, S> newEntry = new StateMapEntry<>(
			key,
			namespace,
			null,
			hash,
			// table[index] 当做 next，明显是头插法
			table[index],
			stateMapVersion,
			stateMapVersion);
		table[index] = newEntry;

		if (table == primaryTable) {
			++primaryTableSize;
		} else {
			++incrementalRehashTableSize;
		}
		return newEntry;
	}

	/**
	 * Select the sub-table which is responsible for entries with the given hash code.
	 *
	 * @param hashCode the hash code which we use to decide about the table that is responsible.
	 * @return the index of the sub-table that is responsible for the entry with the given hash code.
	 * 选择当前元素到底使用 primaryTable 还是 incrementalRehashTable
	 */
	private StateMapEntry<K, N, S>[] selectActiveTable(int hashCode) {
		// 计算 hashCode 应该被分到 primaryTable 的哪个桶中
		int curIndex = hashCode & (primaryTable.length - 1);
		// 大于等于 rehashIndex 的桶还未迁移，应该去 primaryTable 中去查找。
		// 小于 rehashIndex 的桶已经迁移完成，应该去 incrementalRehashTable 中去查找。
		return curIndex >= rehashIndex ? primaryTable : incrementalRehashTable;
	}

	/**
	 * Doubles the capacity of the hash table. Existing entries are placed in
	 * the correct bucket on the enlarged table. If the current capacity is,
	 * MAXIMUM_CAPACITY, this method is a no-op. Returns the table, which
	 * will be new unless we were already at MAXIMUM_CAPACITY.
	 */
	private void doubleCapacity() {

		// There can only be one rehash in flight. From the amount of incremental rehash steps we take, this should always hold.
		Preconditions.checkState(!isRehashing(), "There is already a rehash in progress.");

		StateMapEntry<K, N, S>[] oldMap = primaryTable;

		int oldCapacity = oldMap.length;

		if (oldCapacity == MAXIMUM_CAPACITY) {
			return;
		}

		incrementalRehashTable = makeTable(oldCapacity * 2);
	}

	/**
	 * Returns true, if an incremental rehash is in progress.
	 */
	@VisibleForTesting
	boolean isRehashing() {
		// if we rehash, the secondary table is not empty
		return EMPTY_TABLE != incrementalRehashTable;
	}

	/**
	 * Computes the hash for the composite of key and namespace and performs some steps of incremental rehash if
	 * incremental rehashing is in progress.
	 * 计算 key 和 namespace 对应的 hash 值，如果当前 处于 Rehashing ，则 顺便迁移一波
	 * get、put、containsKey、remove 这四个操作时，会调用 该方法
	 */
	private int computeHashForOperationAndDoIncrementalRehash(K key, N namespace) {

		// Increment table 不为 EMPTY_TABLE，则迁移一波
		if (isRehashing()) {
			incrementalRehash();
		}

		return compositeHash(key, namespace);
	}

	/**
	 * Runs a number of steps for incremental rehashing.
	 */
	@SuppressWarnings("unchecked")
	private void incrementalRehash() {

		StateMapEntry<K, N, S>[] oldMap = primaryTable;
		StateMapEntry<K, N, S>[] newMap = incrementalRehashTable;

		int oldCapacity = oldMap.length;
		int newMask = newMap.length - 1;
		int requiredVersion = highestRequiredSnapshotVersion;
		int rhIdx = rehashIndex;
		// 记录本次迁移了几个元素
		int transferred = 0;

		// we migrate a certain minimum amount of entries from the old to the new table
		// 每次至少迁移 MIN_TRANSFERRED_PER_INCREMENTAL_REHASH 个元素到新桶、
		// MIN_TRANSFERRED_PER_INCREMENTAL_REHASH 默认为 4
		while (transferred < MIN_TRANSFERRED_PER_INCREMENTAL_REHASH) {

			// 遍历 oldMap 的第 rhIdx 个桶
			StateMapEntry<K, N, S> e = oldMap[rhIdx];

			// 每次 e 都指向 e.next，e 不为空，表示当前桶中还有元素未遍历，需要继续遍历
			// 每次迁移必须保证，整个桶被迁移完，不能是某个桶迁移到一半
			while (e != null) {
				// copy-on-write check for entry
				// 遇到版本比 highestRequiredSnapshotVersion 小的元素，则 copy 一份
				if (e.entryVersion < requiredVersion) {
					e = new StateMapEntry<>(e, stateMapVersion);
				}
				// 保存下一个要迁移的节点节点到 n
				StateMapEntry<K, N, S> n = e.next;

				// 迁移当前元素 e 到新的 table 中，插入到链表头部
				int pos = e.hash & newMask;
				e.next = newMap[pos];
				newMap[pos] = e;

				// e 指向下一个要迁移的节点
				e = n;
				// 迁移元素数 +1
				++transferred;
			}

			// oldMap 中 rhIdx 所在的 桶清空
			// rhIdx 之前的桶已经迁移完，rhIdx == oldCapacity 就表示迁移完成了
			oldMap[rhIdx] = null;
			if (++rhIdx == oldCapacity) {
				//here, the rehash is complete and we release resources and reset fields
				primaryTable = newMap;
				incrementalRehashTable = (StateMapEntry<K, N, S>[]) EMPTY_TABLE;
				primaryTableSize += incrementalRehashTableSize;
				incrementalRehashTableSize = 0;
				rehashIndex = 0;
				return;
			}
		}

		// sync our local bookkeeping the with official bookkeeping fields
		// primaryTableSize 中减去 transferred，增加 transferred
		primaryTableSize -= transferred;
		incrementalRehashTableSize += transferred;
		rehashIndex = rhIdx;
	}

	/**
	 * Perform copy-on-write for entry chains. We iterate the (hopefully and probably) still cached chain, replace
	 * all links up to the 'untilEntry', which we actually wanted to modify.
	 * 将当前桶中 untilEntry 之前的所有元素中
	 * current.entryVersion 小于 highestRequiredSnapshotVersion 的元素全部 copy 一份。
	 * entryVersion 大于等于 highestRequiredSnapshotVersion 的元素原样保留即可。
	 * 将这个新链表保存到桶中
	 */
	private StateMapEntry<K, N, S> handleChainedEntryCopyOnWrite(
		StateMapEntry<K, N, S>[] tab,
		int mapIdx,
		StateMapEntry<K, N, S> untilEntry) {

		final int required = highestRequiredSnapshotVersion;

		// current 指向当前桶的头结点
		StateMapEntry<K, N, S> current = tab[mapIdx];
		StateMapEntry<K, N, S> copy;

		// 判断头结点创建时的版本是否低于 highestRequiredSnapshotVersion
		// 如果低于，则 current 节点被 Snapshot 引用，所以需要 new 一个新的 Entry
		if (current.entryVersion < required) {
			copy = new StateMapEntry<>(current, stateMapVersion);
			tab[mapIdx] = copy;
		} else {
			// nothing to do, just advance copy to current
			copy = current;
		}

		// we iterate the chain up to 'until entry'
		// 依次遍历当前桶的元素，直到遍历到 untilEntry 节点，也就是我们要修改的 Entry 节点
		while (current != untilEntry) {

			//advance current
			current = current.next;

			// current 版本小于 highestRequiredSnapshotVersion，则需要拷贝，
			// 否则不用拷贝
			if (current.entryVersion < required) {
				// copy and advance the current's copy
				// entryVersion 表示创建 Entry 时的 version，
				// 所以新创建的 Entry 对应的 entryVersion 要更新为当前 StateMap 的 version
				copy.next = new StateMapEntry<>(current, stateMapVersion);
				copy = copy.next;
			} else {
				// nothing to do, just advance copy to current
				copy = current;
			}
		}

		return copy;
	}

	@SuppressWarnings("unchecked")
	private static <K, N, S> StateMapEntry<K, N, S> getBootstrapEntry() {
		return (StateMapEntry<K, N, S>) ITERATOR_BOOTSTRAP_ENTRY;
	}

	/**
	 * Helper function that creates and scrambles a composite hash for key and namespace.
	 */
	private static int compositeHash(Object key, Object namespace) {
		// create composite key through XOR, then apply some bit-mixing for better distribution of skewed keys.
//		return MathUtils.bitMix(key.hashCode() ^ namespace.hashCode());
		return 0;
	}

	/**
	 * Creates a snapshot of this {@link CopyOnWriteStateMap}, to be written in checkpointing. The snapshot integrity
	 * is protected through copy-on-write from the {@link CopyOnWriteStateMap}. Users should call
	 * {@link #releaseSnapshot(StateMapSnapshot)} after using the returned object.
	 *
	 * @return a snapshot from this {@link CopyOnWriteStateMap}, for checkpointing.
	 */
	@Nonnull
	@Override
	public CopyOnWriteStateMapSnapshot<K, N, S> stateSnapshot() {
		return new CopyOnWriteStateMapSnapshot<>(this);
	}

	/**
	 * Releases a snapshot for this {@link CopyOnWriteStateMap}. This method should be called once a snapshot is no more needed,
	 * so that the {@link CopyOnWriteStateMap} can stop considering this snapshot for copy-on-write, thus avoiding unnecessary
	 * object creation.
	 *
	 * @param snapshotToRelease the snapshot to release, which was previously created by this state map.
	 */
	@Override
	public void releaseSnapshot(StateMapSnapshot<K, N, S, ? extends StateMap<K, N, S>> snapshotToRelease) {

		CopyOnWriteStateMapSnapshot<K, N, S> copyOnWriteStateMapSnapshot = (CopyOnWriteStateMapSnapshot<K, N, S>) snapshotToRelease;

			Preconditions.checkArgument(copyOnWriteStateMapSnapshot.isOwner(this),
			"Cannot release snapshot which is owned by a different state map.");

		releaseSnapshot(copyOnWriteStateMapSnapshot.getSnapshotVersion());
	}

	// Meta data setter / getter and toString -----------------------------------------------------

	public TypeSerializer<S> getStateSerializer() {
		return stateSerializer;
	}

	// StateMapEntry -------------------------------------------------------------------------------------------------

	/**
	 * One entry in the {@link CopyOnWriteStateMap}. This is a triplet of key, namespace, and state. Thereby, key and
	 * namespace together serve as a composite key for the state. This class also contains some management meta data for
	 * copy-on-write, a pointer to link other {@link StateMapEntry}s to a list, and cached hash code.
	 *
	 * @param <K> type of key.
	 * @param <N> type of namespace.
	 * @param <S> type of state.
	 */
	@VisibleForTesting
	protected static class StateMapEntry<K, N, S> implements StateEntry<K, N, S> {

		/**
		 * The key. Assumed to be immumap and not null.
		 */
		@Nonnull
		final K key;

		/**
		 * The namespace. Assumed to be immumap and not null.
		 */
		@Nonnull
		final N namespace;

		/**
		 * The state. This is not final to allow exchanging the object for copy-on-write. Can be null.
		 * state 就是 map 中的 value
		 */
		@Nullable
		S state;

		/**
		 * Link to another {@link StateMapEntry}. This is used to resolve collisions in the
		 * {@link CopyOnWriteStateMap} through chaining.
		 * 桶中链表的 next 指针
		 */
		@Nullable
		StateMapEntry<K, N, S> next;

		/**
		 * The version of this {@link StateMapEntry}. This is meta data for copy-on-write of the map structure.
		 * entry 创建时的版本号
		 */
		int entryVersion;

		/**
		 * The version of the state object in this entry. This is meta data for copy-on-write of the state object itself.
		 * state （数据）更新时的 版本号
		 */
		int stateVersion;

		/**
		 * The computed secondary hash for the composite of key and namespace.
		 */
		final int hash;

		StateMapEntry(StateMapEntry<K, N, S> other, int entryVersion) {
			this(other.key, other.namespace, other.state, other.hash, other.next, entryVersion, other.stateVersion);
		}

		StateMapEntry(
			@Nonnull K key,
			@Nonnull N namespace,
			@Nullable S state,
			int hash,
			@Nullable StateMapEntry<K, N, S> next,
			int entryVersion,
			int stateVersion) {
			this.key = key;
			this.namespace = namespace;
			this.hash = hash;
			this.next = next;
			this.entryVersion = entryVersion;
			this.state = state;
			this.stateVersion = stateVersion;
		}

		public final void setState(@Nullable S value, int mapVersion) {
			// naturally, we can update the state version every time we replace the old state with a different object
			if (value != state) {
				this.state = value;
				this.stateVersion = mapVersion;
			}
		}

		@Nonnull
		@Override
		public K getKey() {
			return key;
		}

		@Nonnull
		@Override
		public N getNamespace() {
			return namespace;
		}

		@Nullable
		@Override
		public S getState() {
			return state;
		}

		@Override
		public final boolean equals(Object o) {
			if (!(o instanceof CopyOnWriteStateMap.StateMapEntry)) {
				return false;
			}

			StateEntry<?, ?, ?> e = (StateEntry<?, ?, ?>) o;
			return e.getKey().equals(key)
				&& e.getNamespace().equals(namespace)
				&& Objects.equals(e.getState(), state);
		}

		@Override
		public final int hashCode() {
			return (key.hashCode() ^ namespace.hashCode()) ^ Objects.hashCode(state);
		}

		@Override
		public final String toString() {
			return "(" + key + "|" + namespace + ")=" + state;
		}
	}

	// For testing  ----------------------------------------------------------------------------------------------------

	@Override
	public int sizeOfNamespace(Object namespace) {
		int count = 0;
		for (StateEntry<K, N, S> entry : this) {
			if (null != entry && namespace.equals(entry.getNamespace())) {
				++count;
			}
		}
		return count;
	}


	// StateEntryIterator  ---------------------------------------------------------------------------------------------

	@Override
	public InternalKvState.StateIncrementalVisitor<K, N, S> getStateIncrementalVisitor(int recommendedMaxNumberOfReturnedRecords) {
		return new StateIncrementalVisitorImpl(recommendedMaxNumberOfReturnedRecords);
	}

	/**
	 * Iterator over state entry chains in a {@link CopyOnWriteStateMap}.
	 */
	class StateEntryChainIterator implements Iterator<StateMapEntry<K, N, S>> {
		StateMapEntry<K, N, S>[] activeTable;
		private int nextMapPosition;
		private final int maxTraversedMapPositions;

		StateEntryChainIterator() {
			this(Integer.MAX_VALUE);
		}

		StateEntryChainIterator(int maxTraversedMapPositions) {
			this.maxTraversedMapPositions = maxTraversedMapPositions;
			this.activeTable = primaryTable;
			this.nextMapPosition = 0;
		}

		@Override
		public boolean hasNext() {
			return size() > 0 && (nextMapPosition < activeTable.length || activeTable == primaryTable);
		}

		@Override
		public StateMapEntry<K, N, S> next() {
			StateMapEntry<K, N, S> next;
			// consider both sub-tables to cover the case of rehash
			while (true) { // current is empty
				// try get next in active table or
				// iteration is done over primary and rehash table
				// or primary was swapped with rehash when rehash is done
				next = nextActiveMapPosition();
				if (next != null ||
					nextMapPosition < activeTable.length ||
					activeTable == incrementalRehashTable ||
					activeTable != primaryTable) {
					return next;
				} else {
					// switch to rehash (empty if no rehash)
					activeTable = incrementalRehashTable;
					nextMapPosition = 0;
				}
			}
		}

		private StateMapEntry<K, N, S> nextActiveMapPosition() {
			StateMapEntry<K, N, S>[] tab = activeTable;
			int traversedPositions = 0;
			while (nextMapPosition < tab.length && traversedPositions < maxTraversedMapPositions) {
				StateMapEntry<K, N, S> next = tab[nextMapPosition++];
				if (next != null) {
					return next;
				}
				traversedPositions++;
			}
			return null;
		}
	}

	/**
	 * Iterator over state entries in a {@link CopyOnWriteStateMap} which does not tolerate concurrent modifications.
	 */
	class StateEntryIterator implements Iterator<StateEntry<K, N, S>> {

		private final StateEntryChainIterator chainIterator;
		private StateMapEntry<K, N, S> nextEntry;
		private final int expectedModCount;

		StateEntryIterator() {
			this.chainIterator = new StateEntryChainIterator();
			this.expectedModCount = modCount;
			this.nextEntry = getBootstrapEntry();
			advanceIterator();
		}

		@Override
		public boolean hasNext() {
			return nextEntry != null;
		}

		@Override
		public StateEntry<K, N, S> next() {
			// 迭代过程中，数据发生了修改，则 抛出异常。换言之，数据时不允许被修改的
			if (modCount != expectedModCount) {
				throw new ConcurrentModificationException();
			}
			if (!hasNext()) {
				throw new NoSuchElementException();
			}
			return advanceIterator();
		}

		StateMapEntry<K, N, S> advanceIterator() {
			StateMapEntry<K, N, S> entryToReturn = nextEntry;
			StateMapEntry<K, N, S> next = nextEntry.next;
			if (next == null) {
				next = chainIterator.next();
			}
			nextEntry = next;
			return entryToReturn;
		}
	}

	/**
	 * Incremental visitor over state entries in a {@link CopyOnWriteStateMap}.
	 */
	class StateIncrementalVisitorImpl implements InternalKvState.StateIncrementalVisitor<K, N, S> {

		private final StateEntryChainIterator chainIterator;
		private final Collection<StateEntry<K, N, S>> chainToReturn = new ArrayList<>(5);

		StateIncrementalVisitorImpl(int recommendedMaxNumberOfReturnedRecords) {
			chainIterator = new StateEntryChainIterator(recommendedMaxNumberOfReturnedRecords);
		}

		@Override
		public boolean hasNext() {
			return chainIterator.hasNext();
		}

		@Override
		public Collection<StateEntry<K, N, S>> nextEntries() {
			if (!hasNext()) {
				return null;
			}

			chainToReturn.clear();
			for (StateMapEntry<K, N, S> nextEntry = chainIterator.next();
					nextEntry != null;
					nextEntry = nextEntry.next) {
				chainToReturn.add(nextEntry);
			}
			return chainToReturn;
		}

		@Override
		public void remove(StateEntry<K, N, S> stateEntry) {
			CopyOnWriteStateMap.this.remove(stateEntry.getKey(), stateEntry.getNamespace());
		}

		@Override
		public void update(StateEntry<K, N, S> stateEntry, S newValue) {
			CopyOnWriteStateMap.this.put(stateEntry.getKey(), stateEntry.getNamespace(), newValue);
		}
	}
}
