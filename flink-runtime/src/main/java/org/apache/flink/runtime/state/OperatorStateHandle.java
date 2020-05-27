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

package org.apache.flink.runtime.state;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.util.Preconditions;

import java.io.IOException;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Map;

/**
 * Interface of a state handle for operator state.
 */
public interface OperatorStateHandle extends StreamStateHandle {

	/**
	 * Returns a map of meta data for all contained states by their name.
	 */
	Map<String, StateMetaInfo> getStateNameToPartitionOffsets();

	/**
	 * Returns an input stream to read the operator state information.
	 */
	@Override
	FSDataInputStream openInputStream() throws IOException;

	/**
	 * Returns the underlying stream state handle that points to the state data.
	 */
	StreamStateHandle getDelegateStateHandle();

	/**
	 * The modes that determine how an {@link OperatorStreamStateHandle} is assigned to tasks during restore.
	 * OperatorState 分布模式的枚举
	 */
	enum Mode {
		// 对应 getListState API
		SPLIT_DISTRIBUTE,	// The operator state partitions in the state handle are split and distributed to one task each.
		// 对应 getUnionListState API
		UNION,				// The operator state partitions are UNION-ed upon restoring and sent to all tasks.
		// 对应 BroadcastState
		BROADCAST			// The operator states are identical, as the state is produced from a broadcast stream.
	}

	/**
	 * Meta information about the operator state handle.
	 * operator state handle 主要是 要恢复数据对应的 offset，
	 * 然后根据 offset 和指定的反序列化器进行反序列化即可拿到数据
	 */
	class StateMetaInfo implements Serializable {

		private static final long serialVersionUID = 3593817615858941166L;

		// 当前 State 在状态文件所处的 offset 和 Mode
		private final long[] offsets;
		// OperatorState 的分布模式
		private final Mode distributionMode;

		public StateMetaInfo(long[] offsets, Mode distributionMode) {
			this.offsets = Preconditions.checkNotNull(offsets);
			this.distributionMode = Preconditions.checkNotNull(distributionMode);
		}

		public long[] getOffsets() {
			return offsets;
		}

		public Mode getDistributionMode() {
			return distributionMode;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}

			StateMetaInfo that = (StateMetaInfo) o;

			return Arrays.equals(getOffsets(), that.getOffsets())
				&& getDistributionMode() == that.getDistributionMode();
		}

		@Override
		public int hashCode() {
			int result = Arrays.hashCode(getOffsets());
			result = 31 * result + getDistributionMode().hashCode();
			return result;
		}

		@Override
		public String toString() {
			return "StateMetaInfo{" +
					"offsets=" + Arrays.toString(offsets) +
					", distributionMode=" + distributionMode +
					'}';
		}
	}
}
