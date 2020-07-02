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

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.state.StateEntry;
import org.apache.flink.runtime.state.StateSnapshotTransformer;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

/**
 * This class represents the snapshot of a {@link CopyOnWriteStateMap}.
 *
 * <p>IMPORTANT: Please notice that snapshot integrity of entries in this class rely on proper copy-on-write semantics
 * through the {@link CopyOnWriteStateMap} that created the snapshot object, but all objects in this snapshot must be considered
 * as READ-ONLY!. The reason is that the objects held by this class may or may not be deep copies of original objects
 * that may still used in the {@link CopyOnWriteStateMap}. This depends for each entry on whether or not it was subject to
 * copy-on-write operations by the {@link CopyOnWriteStateMap}. Phrased differently: the {@link CopyOnWriteStateMap} provides
 * copy-on-write isolation for this snapshot, but this snapshot does not isolate modifications from the
 * {@link CopyOnWriteStateMap}!
 *
 * @param <K> type of key
 * @param <N> type of namespace
 * @param <S> type of state
 */
public class CopyOnWriteStateMapSnapshot<K, N, S>
	extends StateMapSnapshot<K, N, S, CopyOnWriteStateMap<K, N, S>> {

	/**
	 * Version of the {@link CopyOnWriteStateMap} when this snapshot was created. This can be used to release the snapshot.
	 * snapshot 创建时的 StateMap 对应的 version
	 */
	private final int snapshotVersion;

	/**
	 * The state map entries, as by the time this snapshot was created. Objects in this array may or may not be deep
	 * copies of the current entries in the {@link CopyOnWriteStateMap} that created this snapshot. This depends for each entry
	 * on whether or not it was subject to copy-on-write operations by the {@link CopyOnWriteStateMap}.
	 * 将快照时的数据全部保存到 snapshotData，之后这些引用关系也不会被打散，
	 * 因为修改了新数据，也不会改这些数据，除非这个 snapshot release 了
	 */
	@Nonnull
	private final CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;

	/**
	 * The number of (non-null) entries in snapshotData
	 * snapshot 创建时的 StateMap 中包含的元素个数
	 */
	@Nonnegative
	private final int numberOfEntriesInSnapshotData;

	/**
	 * Creates a new {@link CopyOnWriteStateMapSnapshot}.
	 *
	 * @param owningStateMap the {@link CopyOnWriteStateMap} for which this object represents a snapshot.
	 */
	CopyOnWriteStateMapSnapshot(CopyOnWriteStateMap<K, N, S> owningStateMap) {
		super(owningStateMap);

		// 对 StateMap 的数据进行浅拷贝，生成 snapshotData
		this.snapshotData = owningStateMap.snapshotMapArrays();
		// 记录当前的 StateMap 版本到 snapshotVersion 中
		this.snapshotVersion = owningStateMap.getStateMapVersion();
		this.numberOfEntriesInSnapshotData = owningStateMap.size();
	}

	@Override
	public void release() {
		owningStateMap.releaseSnapshot(this);
	}

	/**
	 * Returns the internal version of the {@link CopyOnWriteStateMap} when this snapshot was created. This value must be used to
	 * tell the {@link CopyOnWriteStateMap} when to release this snapshot.
	 */
	int getSnapshotVersion() {
		return snapshotVersion;
	}

	// 这里是具体做 snapshot 的流程
	@Override
	public void writeState(
		TypeSerializer<K> keySerializer,
		TypeSerializer<N> namespaceSerializer,
		TypeSerializer<S> stateSerializer,
		@Nonnull DataOutputView dov,
		@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) throws IOException {
		SnapshotIterator<K, N, S> snapshotIterator = stateSnapshotTransformer == null ?
			new NonTransformSnapshotIterator<>(numberOfEntriesInSnapshotData, snapshotData) :
			new TransformedSnapshotIterator<>(numberOfEntriesInSnapshotData, snapshotData, stateSnapshotTransformer);

		int size = snapshotIterator.size();
		dov.writeInt(size);
		// hasNext 和 next 用来遍历一个个 Entry
		while (snapshotIterator.hasNext()) {
			StateEntry<K, N, S> stateEntry = snapshotIterator.next();
			namespaceSerializer.serialize(stateEntry.getNamespace(), dov);
			keySerializer.serialize(stateEntry.getKey(), dov);
			stateSerializer.serialize(stateEntry.getState(), dov);
		}
	}

	/**
	 * Iterator over state entries in a {@link CopyOnWriteStateMapSnapshot}.
	 */
	abstract static class SnapshotIterator<K, N, S> implements Iterator<StateEntry<K, N, S>> {

		int numberOfEntriesInSnapshotData;

		CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData;

		// hashmap 桶级别的迭代器，每次找到下一个有数据的桶
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> chainIterator;

		// 迭代桶中链表元素
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> entryIterator;

		SnapshotIterator(
			int numberOfEntriesInSnapshotData,
			CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData,
			@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
			this.numberOfEntriesInSnapshotData = numberOfEntriesInSnapshotData;
			this.snapshotData = snapshotData;

			transform(stateSnapshotTransformer);
			this.chainIterator = getChainIterator();
			this.entryIterator = Collections.emptyIterator();
		}

		/**
		 * Return the number of state entries in this snapshot.
		 */
		abstract int size();

		/**
		 * Transform the state in the snapshot before iterating the state.
		 */
		abstract void transform(@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer);

		/**
		 * Return an iterator over the chains of entries in snapshotData.
		 */
		abstract Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getChainIterator();

		/**
		 * Return an iterator over the entries in the chain.
		 *
		 * @param stateMapEntry The head entry of the chain.
		 */
		abstract Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getEntryIterator(
			CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry);

		@Override
		public boolean hasNext() {
			return entryIterator.hasNext() || chainIterator.hasNext();
		}

		@Override
		public CopyOnWriteStateMap.StateMapEntry<K, N, S> next() {
			// 当前桶的链表还有元素，则遍历当前 链表，否则使用 Chain 迭代器遍历下一个有数据的桶
			if (entryIterator.hasNext()) {
				return entryIterator.next();
			}

			// 迭代下一个有数据的桶中的链表，并为其链表搞一个迭代器
			CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry = chainIterator.next();
			entryIterator = getEntryIterator(stateMapEntry);
			return entryIterator.next();
		}
	}

	/**
	 * Implementation of {@link SnapshotIterator} with no transform.
	 */
	static class NonTransformSnapshotIterator<K, N, S> extends SnapshotIterator<K, N, S> {

		NonTransformSnapshotIterator(
			int numberOfEntriesInSnapshotData,
			CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData) {
			super(numberOfEntriesInSnapshotData, snapshotData, null);
		}

		@Override
		void transform(@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
		}

		@Override
		public int size() {
			return numberOfEntriesInSnapshotData;
		}

		@Override
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getChainIterator() {
			return Arrays.stream(snapshotData).filter(Objects::nonNull).iterator();
		}

		//
		@Override
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getEntryIterator(
			final CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry) {
			return new Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>>() {

				CopyOnWriteStateMap.StateMapEntry<K, N, S> nextEntry = stateMapEntry;

				@Override
				public boolean hasNext() {
					return nextEntry != null;
				}

				@Override
				public CopyOnWriteStateMap.StateMapEntry<K, N, S> next() {
					if (nextEntry == null) {
						throw new NoSuchElementException();
					}
					CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = nextEntry;
					nextEntry = nextEntry.next;
					return entry;
				}
			};
		}
	}

	/**
	 * Implementation of {@link SnapshotIterator} with a {@link StateSnapshotTransformer}.
	 */
	static class TransformedSnapshotIterator<K, N, S> extends SnapshotIterator<K, N, S> {

		TransformedSnapshotIterator(
			int numberOfEntriesInSnapshotData,
			CopyOnWriteStateMap.StateMapEntry<K, N, S>[] snapshotData,
			@Nonnull StateSnapshotTransformer<S> stateSnapshotTransformer) {
			super(numberOfEntriesInSnapshotData, snapshotData, stateSnapshotTransformer);
		}

		/**
		 * Move the chains in snapshotData to the back of the array, and return the
		 * index of the first chain from the front.
		 * 将所有非空桶中的整个链，全部移动的数组的尾部，只是移动了位置，并没有将链中的数据依次遍历出来
		 */
		int moveChainsToBackOfArray() {
			int index = snapshotData.length - 1;
			// find the first null chain from the back
			while (index >= 0) {
				if (snapshotData[index] == null) {
					break;
				}
				index--;
			}

			int lastNullIndex = index;
			index--;
			// move the chains to the back
			while (index >= 0) {
				CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = snapshotData[index];
				if (entry != null) {
					snapshotData[lastNullIndex] = entry;
					snapshotData[index] = null;
					lastNullIndex--;
				}
				index--;
			}
			// return the index of the first chain from the front
			return lastNullIndex + 1;
		}

		@Override
		void transform(@Nullable StateSnapshotTransformer<S> stateSnapshotTransformer) {
			Preconditions.checkNotNull(stateSnapshotTransformer);
			// 将所有非空桶中的整个链，全部移动的数组的尾部，只是移动了位置，并没有将链中的数据依次遍历出来
			int indexOfFirstChain = moveChainsToBackOfArray();
			int count = 0;
			// 从有数据的桶开始遍历元素，将所有元素拍平，放到 数组的头部
			// reuse the snapshotData to transform and flatten the entries.
			for (int i = indexOfFirstChain; i < snapshotData.length; i++) {
				CopyOnWriteStateMap.StateMapEntry<K, N, S> entry = snapshotData[i];
				while (entry != null) {
					S transformedValue = stateSnapshotTransformer.filterOrTransform(entry.state);
					if (transformedValue != null) {
						CopyOnWriteStateMap.StateMapEntry<K, N, S> filteredEntry = entry;
						if (transformedValue != entry.state) {
							filteredEntry = new CopyOnWriteStateMap.StateMapEntry<>(entry, entry.entryVersion);
							filteredEntry.state = transformedValue;
						}
						snapshotData[count++] = filteredEntry;
					}
					entry = entry.next;
				}
			}
			numberOfEntriesInSnapshotData = count;
		}

		@Override
		public int size() {
			return numberOfEntriesInSnapshotData;
		}

		@Override
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getChainIterator() {
			return Arrays.stream(snapshotData, 0, numberOfEntriesInSnapshotData).iterator();
		}

		@Override
		Iterator<CopyOnWriteStateMap.StateMapEntry<K, N, S>> getEntryIterator(
			CopyOnWriteStateMap.StateMapEntry<K, N, S> stateMapEntry) {
			return Collections.singleton(stateMapEntry).iterator();
		}
	}
}
