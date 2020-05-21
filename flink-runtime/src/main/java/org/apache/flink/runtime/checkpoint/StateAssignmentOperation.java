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

package org.apache.flink.runtime.checkpoint;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionJobVertex;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.OperatorInstanceID;
import org.apache.flink.runtime.state.KeyGroupRange;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.state.KeyGroupsStateHandle;
import org.apache.flink.runtime.state.KeyedStateHandle;
import org.apache.flink.runtime.state.OperatorStateHandle;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This class encapsulates the operation of assigning restored state when restoring from a checkpoint.
 */
@Internal
public class StateAssignmentOperation {

	private static final Logger LOG = LoggerFactory.getLogger(StateAssignmentOperation.class);

	// 新 Job ExecutionGraph 对应的执行图
	private final Map<JobVertexID, ExecutionJobVertex> tasks;

	// 从 Checkpoint 中恢复的 OperatorID 与 Operator 对应 State 的映射
	private final Map<OperatorID, OperatorState> operatorStates;

	private final long restoreCheckpointId;
	private final boolean allowNonRestoredState;

	public StateAssignmentOperation(
		long restoreCheckpointId,
		Map<JobVertexID, ExecutionJobVertex> tasks,
		Map<OperatorID, OperatorState> operatorStates,
		boolean allowNonRestoredState) {

		this.restoreCheckpointId = restoreCheckpointId;
		this.tasks = Preconditions.checkNotNull(tasks);
		this.operatorStates = Preconditions.checkNotNull(operatorStates);
		this.allowNonRestoredState = allowNonRestoredState;
	}

	// 重点：给 TM 分配 State
	public void assignStates() {
		// localOperators 保存所有的 恢复出来的 State
		Map<OperatorID, OperatorState> localOperators = new HashMap<>(operatorStates);

		// 检查 Checkpoint 中恢复的所有 State 是否可以映射到 ExecutionGraph 上。
		// 如果有 State 匹配不到执行的算子，且用户要求必须能够严格匹配，则抛出异常
		checkStateMappingCompleteness(allowNonRestoredState, operatorStates, tasks);

		// 从最新的 ExecutionGraph 中遍历一个个 task ，一个个 task 去进行 assign
		for (Map.Entry<JobVertexID, ExecutionJobVertex> task : this.tasks.entrySet()) {
			final ExecutionJobVertex executionJobVertex = task.getValue();

			// find the states of all operators belonging to this task
			List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();
			List<OperatorID> altOperatorIDs = executionJobVertex.getUserDefinedOperatorIDs();

			// 记录所有有状态算子 OperatorState
			List<OperatorState> operatorStates = new ArrayList<>(operatorIDs.size());

			boolean statelessTask = true;
			for (int x = 0; x < operatorIDs.size(); x++) {
				// 优先使用 altOperatorID，altOperatorIDs 为空再使用 operatorIDs
				OperatorID operatorID = altOperatorIDs.get(x) == null
					? operatorIDs.get(x)
					: altOperatorIDs.get(x);

				OperatorState operatorState = localOperators.remove(operatorID);

				//  operatorState 为空，表示当前 Operator 没有恢复出来的 State
				if (operatorState == null) {
					// 无状态的 Operator，设置 OperatorID、并行度、最大并行度 这三个指标都与 新的 ExecutionGraph 中数据一致
					operatorState = new OperatorState(
						operatorID,
						executionJobVertex.getParallelism(),
						executionJobVertex.getMaxParallelism());
				} else {
					// 能走到这里，说明当前 task 找到了某个 Operator 的 State
					statelessTask = false;
				}
				operatorStates.add(operatorState);
			}
			// statelessTask == true 表示当前 task 的所有 Operator 都不需要从 State 恢复
			if (statelessTask) { // skip tasks where no operator has any state
				continue;
			}

			// 给 ExecutionGraph 中各个 subtask 分配 StateHandle，包括 Operator 和 Keyed
			assignAttemptState(task.getValue(), operatorStates);
		}

	}

	private void assignAttemptState(ExecutionJobVertex executionJobVertex, List<OperatorState> operatorStates) {

		List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();

		//1. first compute the new parallelism
		// check 并行度相关是否符合规则：
		// 1、 Job 的并行度是否大于 Checkpoint 状态中保存的 最大并行度
		// 2、 判断 MaxParallelism 是否改变：没有改变，则直接跳过
		// 		改变的情况：
		//			如果用户主动配置了 MaxParallelism 则任务不能恢复，
		//			如果 MaxParallelism 改变是因为框架自动改变的，则将 JobVertex 的 MaxParallelism 设置为 State 的 MaxParallelism
		checkParallelismPreconditions(operatorStates, executionJobVertex);

		// 为当前新的 JobVertex 的所有 ExecutionVertex 生成了 KeyGroupRange
		int newParallelism = executionJobVertex.getParallelism();
		List<KeyGroupRange> keyGroupPartitions = createKeyGroupPartitions(
			executionJobVertex.getMaxParallelism(),
			newParallelism);

		final int expectedNumberOfSubTasks = newParallelism * operatorIDs.size();

		/*
		 * Redistribute ManagedOperatorStates and RawOperatorStates from old parallelism to new parallelism.
		 *
		 * The old ManagedOperatorStates with old parallelism 3:
		 *
		 * 		parallelism0 parallelism1 parallelism2
		 * op0   states0,0    state0,1	   state0,2
		 * op1
		 * op2   states2,0    state2,1	   state1,2
		 * op3   states3,0    state3,1     state3,2
		 *
		 * The new ManagedOperatorStates with new parallelism 4:
		 *
		 * 		parallelism0 parallelism1 parallelism2 parallelism3
		 * op0   state0,0	  state0,1 	   state0,2		state0,3
		 * op1
		 * op2   state2,0	  state2,1 	   state2,2		state2,3
		 * op3   state3,0	  state3,1 	   state3,2		state3,3
		 */
		// 给当前 JobVertex 的所有 ExecutionVertex 重新分配 OperatorState
		Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates =
			new HashMap<>(expectedNumberOfSubTasks);
		Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates =
			new HashMap<>(expectedNumberOfSubTasks);

		reDistributePartitionableStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			newManagedOperatorStates,
			newRawOperatorStates);

		// 给当前 JobVertex 的所有 ExecutionVertex 重新分配 KeyedState
		Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState =
			new HashMap<>(expectedNumberOfSubTasks);
		Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState =
			new HashMap<>(expectedNumberOfSubTasks);

		reDistributeKeyedStates(
			operatorStates,
			newParallelism,
			operatorIDs,
			keyGroupPartitions,
			newManagedKeyedState,
			newRawKeyedState);

		/*
		 *  An executionJobVertex's all state handles needed to restore are something like a matrix
		 * 将 四种 StateHandle 封装到 ExecutionJobVertex 中。
		 *
		 * 		parallelism0 parallelism1 parallelism2 parallelism3
		 * op0   sh(0,0)     sh(0,1)       sh(0,2)	    sh(0,3)
		 * op1   sh(1,0)	 sh(1,1)	   sh(1,2)	    sh(1,3)
		 * op2   sh(2,0)	 sh(2,1)	   sh(2,2)		sh(2,3)
		 * op3   sh(3,0)	 sh(3,1)	   sh(3,2)		sh(3,3)
		 *
		 */
		assignTaskStateToExecutionJobVertices(
			executionJobVertex,
			newManagedOperatorStates,
			newRawOperatorStates,
			newManagedKeyedState,
			newRawKeyedState,
			newParallelism);
	}

	private void assignTaskStateToExecutionJobVertices(
			ExecutionJobVertex executionJobVertex,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState,
			int newParallelism) {

		List<OperatorID> operatorIDs = executionJobVertex.getOperatorIDs();

		for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {

			Execution currentExecutionAttempt = executionJobVertex.getTaskVertices()[subTaskIndex]
				.getCurrentExecutionAttempt();

			TaskStateSnapshot taskState = new TaskStateSnapshot(operatorIDs.size());
			boolean statelessTask = true;

			for (OperatorID operatorID : operatorIDs) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, operatorID);

				OperatorSubtaskState operatorSubtaskState = operatorSubtaskStateFrom(
					instanceID,
					subManagedOperatorState,
					subRawOperatorState,
					subManagedKeyedState,
					subRawKeyedState);

				if (operatorSubtaskState.hasState()) {
					statelessTask = false;
				}
				taskState.putSubtaskStateByOperatorID(operatorID, operatorSubtaskState);
			}

			if (!statelessTask) {
				JobManagerTaskRestore taskRestore = new JobManagerTaskRestore(restoreCheckpointId, taskState);
				currentExecutionAttempt.setInitialState(taskRestore);
			}
		}
	}

	public static OperatorSubtaskState operatorSubtaskStateFrom(
			OperatorInstanceID instanceID,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subManagedOperatorState,
			Map<OperatorInstanceID, List<OperatorStateHandle>> subRawOperatorState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subManagedKeyedState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> subRawKeyedState) {

		if (!subManagedOperatorState.containsKey(instanceID) &&
			!subRawOperatorState.containsKey(instanceID) &&
			!subManagedKeyedState.containsKey(instanceID) &&
			!subRawKeyedState.containsKey(instanceID)) {

			return new OperatorSubtaskState();
		}
		if (!subManagedKeyedState.containsKey(instanceID)) {
			checkState(!subRawKeyedState.containsKey(instanceID));
		}
		return new OperatorSubtaskState(
			new StateObjectCollection<>(subManagedOperatorState.getOrDefault(instanceID, Collections.emptyList())),
			new StateObjectCollection<>(subRawOperatorState.getOrDefault(instanceID, Collections.emptyList())),
			new StateObjectCollection<>(subManagedKeyedState.getOrDefault(instanceID, Collections.emptyList())),
			new StateObjectCollection<>(subRawKeyedState.getOrDefault(instanceID, Collections.emptyList())));
	}

	public void checkParallelismPreconditions(List<OperatorState> operatorStates, ExecutionJobVertex executionJobVertex) {
		for (OperatorState operatorState : operatorStates) {
			checkParallelismPreconditions(operatorState, executionJobVertex);
		}
	}

	private void reDistributeKeyedStates(
			List<OperatorState> oldOperatorStates,
			int newParallelism,
			List<OperatorID> newOperatorIDs,
			List<KeyGroupRange> newKeyGroupPartitions,
			Map<OperatorInstanceID, List<KeyedStateHandle>> newManagedKeyedState,
			Map<OperatorInstanceID, List<KeyedStateHandle>> newRawKeyedState) {
		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		// 遍历一个个 Operator 算子
		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
			int oldParallelism = operatorState.getParallelism();
			// 遍历一个个新的 subtask，看应该分配哪些 StateHandle 给这些新的 subtask
			for (int subTaskIndex = 0; subTaskIndex < newParallelism; subTaskIndex++) {
				OperatorInstanceID instanceID = OperatorInstanceID.of(subTaskIndex, newOperatorIDs.get(operatorIndex));

				Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> subKeyedStates = reAssignSubKeyedStates(
					operatorState,
					newKeyGroupPartitions,
					subTaskIndex,
					newParallelism,
					oldParallelism);

				newManagedKeyedState.put(instanceID, subKeyedStates.f0);
				newRawKeyedState.put(instanceID, subKeyedStates.f1);
			}
		}
	}

	// TODO rewrite based on operator id
	private Tuple2<List<KeyedStateHandle>, List<KeyedStateHandle>> reAssignSubKeyedStates(
			OperatorState operatorState,
			List<KeyGroupRange> keyGroupPartitions,
			int subTaskIndex,
			int newParallelism,
			int oldParallelism) {

		List<KeyedStateHandle> subManagedKeyedState;
		List<KeyedStateHandle> subRawKeyedState;

		// 并行度没有改变，直接按照老的 State 进行分配
		if (newParallelism == oldParallelism) {
			if (operatorState.getState(subTaskIndex) != null) {
				subManagedKeyedState = operatorState.getState(subTaskIndex).getManagedKeyedState().asList();
				subRawKeyedState = operatorState.getState(subTaskIndex).getRawKeyedState().asList();
			} else {
				subManagedKeyedState = Collections.emptyList();
				subRawKeyedState = Collections.emptyList();
			}
		} else {
			// 并行度改变的情况，把当前 OperatorState 的所有 subtask 的 StateHandle 全部遍历一遍，
			// 看是否与 subtaskKeyGroupRange 有交集，有交集则将对应的 Handle 添加到 list 用于恢复
			subManagedKeyedState = getManagedKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
			subRawKeyedState = getRawKeyedStateHandles(operatorState, keyGroupPartitions.get(subTaskIndex));
		}

		if (subManagedKeyedState.isEmpty() && subRawKeyedState.isEmpty()) {
			return new Tuple2<>(Collections.emptyList(), Collections.emptyList());
		} else {
			return new Tuple2<>(subManagedKeyedState, subRawKeyedState);
		}
	}

	public static void reDistributePartitionableStates(
			List<OperatorState> oldOperatorStates,
			int newParallelism,
			List<OperatorID> newOperatorIDs,
			Map<OperatorInstanceID, List<OperatorStateHandle>> newManagedOperatorStates,
			Map<OperatorInstanceID, List<OperatorStateHandle>> newRawOperatorStates) {

		//TODO: rewrite this method to only use OperatorID
		checkState(newOperatorIDs.size() == oldOperatorStates.size(),
			"This method still depends on the order of the new and old operators");

		// The nested list wraps as the level of operator -> subtask -> state object collection
		// 嵌套 list 封装级别是  operator -> subtask -> state object collection
		List<List<List<OperatorStateHandle>>> oldManagedOperatorStates = new ArrayList<>(oldOperatorStates.size());
		List<List<List<OperatorStateHandle>>> oldRawOperatorStates = new ArrayList<>(oldOperatorStates.size());
		// 将 要恢复的 State 按照 Managed 和 Raw 分类到两个 list 中
		splitManagedAndRawOperatorStates(oldOperatorStates, oldManagedOperatorStates, oldRawOperatorStates);

		OperatorStateRepartitioner opStateRepartitioner = RoundRobinOperatorStateRepartitioner.INSTANCE;

		// 遍历一个个 Operator
		for (int operatorIndex = 0; operatorIndex < newOperatorIDs.size(); operatorIndex++) {
			// 获取当前 Operator 对应的 State
			OperatorState operatorState = oldOperatorStates.get(operatorIndex);
			int oldParallelism = operatorState.getParallelism();

			OperatorID operatorID = newOperatorIDs.get(operatorIndex);

			newManagedOperatorStates.putAll(applyRepartitioner(
				operatorID,
				opStateRepartitioner,
				oldManagedOperatorStates.get(operatorIndex),
				oldParallelism,
				newParallelism));

			newRawOperatorStates.putAll(applyRepartitioner(
				operatorID,
				opStateRepartitioner,
				oldRawOperatorStates.get(operatorIndex),
				oldParallelism,
				newParallelism));
		}
	}

	private static void splitManagedAndRawOperatorStates(
		List<OperatorState> operatorStates,
		List<List<List<OperatorStateHandle>>> managedOperatorStates,
		List<List<List<OperatorStateHandle>>> rawOperatorStates) {

		for (OperatorState operatorState : operatorStates) {

			final int parallelism = operatorState.getParallelism();
			List<List<OperatorStateHandle>> managedOpStatePerSubtasks = new ArrayList<>(parallelism);
			List<List<OperatorStateHandle>> rawOpStatePerSubtasks = new ArrayList<>(parallelism);

			for (int subTaskIndex = 0; subTaskIndex < parallelism; subTaskIndex++) {
				OperatorSubtaskState operatorSubtaskState = operatorState.getState(subTaskIndex);
				if (operatorSubtaskState == null) {
					managedOpStatePerSubtasks.add(Collections.emptyList());
					rawOpStatePerSubtasks.add(Collections.emptyList());
				} else {
					StateObjectCollection<OperatorStateHandle> managed = operatorSubtaskState.getManagedOperatorState();
					StateObjectCollection<OperatorStateHandle> raw = operatorSubtaskState.getRawOperatorState();

					managedOpStatePerSubtasks.add(managed.asList());
					rawOpStatePerSubtasks.add(raw.asList());
				}
			}
			managedOperatorStates.add(managedOpStatePerSubtasks);
			rawOperatorStates.add(rawOpStatePerSubtasks);
		}
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  managedKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState        all state handles of a operator：当前 Operator 所有恢复的 OperatorState
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask ：当前 subtask 对应的 KeyGroupRange
	 * @return all managedKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getManagedKeyedStateHandles(
		OperatorState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		final int parallelism = operatorState.getParallelism();

		List<KeyedStateHandle> subtaskKeyedStateHandles = null;

		// 把从 Checkpoint 处恢复的 OperatorState 的所有 subtask 全部遍历一遍，
		// 看是否与 subtaskKeyGroupRange 有交集，有交集则对应的 Handle 需要用于恢复
		for (int i = 0; i < parallelism; i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> keyedStateHandles = operatorState.getState(i).getManagedKeyedState();

				if (subtaskKeyedStateHandles == null) {
					subtaskKeyedStateHandles = new ArrayList<>(parallelism * keyedStateHandles.size());
				}

				// 将交集的 StateHandles 加入到 list 中
				extractIntersectingState(
					keyedStateHandles,
					subtaskKeyGroupRange,
					subtaskKeyedStateHandles);
			}
		}

		return subtaskKeyedStateHandles;
	}

	/**
	 * Collect {@link KeyGroupsStateHandle  rawKeyedStateHandles} which have intersection with given
	 * {@link KeyGroupRange} from {@link TaskState operatorState}
	 *
	 * @param operatorState        all state handles of a operator
	 * @param subtaskKeyGroupRange the KeyGroupRange of a subtask
	 * @return all rawKeyedStateHandles which have intersection with given KeyGroupRange
	 */
	public static List<KeyedStateHandle> getRawKeyedStateHandles(
		OperatorState operatorState,
		KeyGroupRange subtaskKeyGroupRange) {

		final int parallelism = operatorState.getParallelism();

		List<KeyedStateHandle> extractedKeyedStateHandles = null;

		for (int i = 0; i < parallelism; i++) {
			if (operatorState.getState(i) != null) {

				Collection<KeyedStateHandle> rawKeyedState = operatorState.getState(i).getRawKeyedState();

				if (extractedKeyedStateHandles == null) {
					extractedKeyedStateHandles = new ArrayList<>(parallelism * rawKeyedState.size());
				}

				extractIntersectingState(
					rawKeyedState,
					subtaskKeyGroupRange,
					extractedKeyedStateHandles);
			}
		}

		return extractedKeyedStateHandles;
	}

	/**
	 * Extracts certain key group ranges from the given state handles and adds them to the collector.
	 * 将 originalSubtaskStateHandles 与 rangeToExtract 有交集的 KeyedStateHandle，
	 * 添加到 extractedStateCollector 集合中
	 * @param originalSubtaskStateHandles
	 * @param rangeToExtract
	 * @param extractedStateCollector
	 */
	private static void extractIntersectingState(
		Collection<KeyedStateHandle> originalSubtaskStateHandles,
		KeyGroupRange rangeToExtract,
		List<KeyedStateHandle> extractedStateCollector) {

		for (KeyedStateHandle keyedStateHandle : originalSubtaskStateHandles) {

			if (keyedStateHandle != null) {

				KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(rangeToExtract);

				if (intersectedKeyedStateHandle != null) {
					extractedStateCollector.add(intersectedKeyedStateHandle);
				}
			}
		}
	}

	/**
	 * Groups the available set of key groups into key group partitions. A key group partition is
	 * the set of key groups which is assigned to the same task. Each set of the returned list
	 * constitutes a key group partition.
	 * <p>
	 * <b>IMPORTANT</b>: The assignment of key groups to partitions has to be in sync with the
	 * KeyGroupStreamPartitioner.
	 *
	 * @param numberKeyGroups Number of available key groups (indexed from 0 to numberKeyGroups - 1)
	 * @param parallelism     Parallelism to generate the key group partitioning for
	 * @return List of key group partitions
	 */
	public static List<KeyGroupRange> createKeyGroupPartitions(int numberKeyGroups, int parallelism) {
		Preconditions.checkArgument(numberKeyGroups >= parallelism);
		List<KeyGroupRange> result = new ArrayList<>(parallelism);

		for (int i = 0; i < parallelism; ++i) {
			result.add(KeyGroupRangeAssignment.computeKeyGroupRangeForOperatorIndex(numberKeyGroups, parallelism, i));
		}
		return result;
	}

	/**
	 * Verifies conditions in regards to parallelism and maxParallelism that must be met when restoring state.
	 *
	 * @param operatorState      state to restore
	 * @param executionJobVertex task for which the state should be restored
	 */
	private static void checkParallelismPreconditions(OperatorState operatorState, ExecutionJobVertex executionJobVertex) {
		//----------------------------------------max parallelism preconditions-------------------------------------

		// ExecutionGraph 的并行度大于 State 设置的 MaxParallelism ，任务不能启动
		if (operatorState.getMaxParallelism() < executionJobVertex.getParallelism()) {
			throw new IllegalStateException("The state for task " + executionJobVertex.getJobVertexId() +
				" can not be restored. The maximum parallelism (" + operatorState.getMaxParallelism() +
				") of the restored state is lower than the configured parallelism (" + executionJobVertex.getParallelism() +
				"). Please reduce the parallelism of the task to be lower or equal to the maximum parallelism."
			);
		}

		// check that the number of key groups have not changed or if we need to override it to satisfy the restored state
		if (operatorState.getMaxParallelism() != executionJobVertex.getMaxParallelism()) {
			// MaxParallelism 改变的情况：
			// 	如果用户主动配置了 MaxParallelism 则任务不能恢复，
			// 	如果 MaxParallelism 改变是因为框架自动改变的，则将 JobVertex 的 MaxParallelism 设置为 State 的 MaxParallelism

			if (!executionJobVertex.isMaxParallelismConfigured()) {
				// if the max parallelism was not explicitly specified by the user, we derive it from the state

				LOG.debug("Overriding maximum parallelism for JobVertex {} from {} to {}",
					executionJobVertex.getJobVertexId(), executionJobVertex.getMaxParallelism(), operatorState.getMaxParallelism());

				executionJobVertex.setMaxParallelism(operatorState.getMaxParallelism());
			} else {
				// if the max parallelism was explicitly specified, we complain on mismatch
				throw new IllegalStateException("The maximum parallelism (" +
					operatorState.getMaxParallelism() + ") with which the latest " +
					"checkpoint of the execution job vertex " + executionJobVertex +
					" has been taken and the current maximum parallelism (" +
					executionJobVertex.getMaxParallelism() + ") changed. This " +
					"is currently not supported.");
			}
		}
	}

	/**
	 * Verifies that all operator states can be mapped to an execution job vertex.
	 * 检查 Checkpoint 中恢复的所有 State 是否可以映射到 ExecutionGraph 上。
	 *
	 * @param allowNonRestoredState if false an exception will be thrown if a state could not be mapped
	 * @param operatorStates operator states to map
	 * @param tasks task to map to
	 */
	private static void checkStateMappingCompleteness(
			boolean allowNonRestoredState,
			Map<OperatorID, OperatorState> operatorStates,
			Map<JobVertexID, ExecutionJobVertex> tasks) {

		// 将新的 ExecutionGraph 所有算子 ID 维护在 Set 中
		Set<OperatorID> allOperatorIDs = new HashSet<>();
		for (ExecutionJobVertex executionJobVertex : tasks.values()) {
			allOperatorIDs.addAll(executionJobVertex.getOperatorIDs());
		}

		/**
		 * 遍历所有的 State，看是否能匹配到 ExecutionGraph 的算子 ID
		 * 能匹配到就是正常，匹配不到，再看配置是否允许匹配不到。
		 * 如果有 State 匹配不到执行的算子，且用户要求必须能够严格匹配，则抛出异常
 		 */
		for (Map.Entry<OperatorID, OperatorState> operatorGroupStateEntry : operatorStates.entrySet()) {
			OperatorState operatorState = operatorGroupStateEntry.getValue();
			//----------------------------------------find operator for state---------------------------------------------

			if (!allOperatorIDs.contains(operatorGroupStateEntry.getKey())) {
				if (allowNonRestoredState) {
					LOG.info("Skipped checkpoint state for operator {}.", operatorState.getOperatorID());
				} else {
					throw new IllegalStateException("There is no operator for the state " + operatorState.getOperatorID());
				}
			}
		}
	}

	/**
	 * 传入 OperatorID，输出 Operator 下每个 subtask 要恢复的 OperatorStateHandle 的集合
	 * @param operatorID operatorID
	 * @param opStateRepartitioner 默认是 RoundRobin 的重分区器
	 * @param chainOpParallelStates 当前 Operator 级别对应的 State，
	 *                              这里是嵌套数组，表示恢复之前每个 subtask 对应的 OperatorStateHandle 的集合
	 * @param oldParallelism oldParallelism
	 * @param newParallelism newParallelism
	 * @return Operator 下每个 subtask 要恢复的 OperatorStateHandle 的集合
	 */
	public static Map<OperatorInstanceID, List<OperatorStateHandle>> applyRepartitioner(
		OperatorID operatorID,
		OperatorStateRepartitioner opStateRepartitioner,
		List<List<OperatorStateHandle>> chainOpParallelStates,
		int oldParallelism,
		int newParallelism) {

		// 返回 每个 subtask 对应的 OperatorStateHandle 的集合
		// 嵌套集合表示：subtask -> OperatorStateHandle 的集合
		List<List<OperatorStateHandle>> states = applyRepartitioner(
			opStateRepartitioner,
			chainOpParallelStates,
			oldParallelism,
			newParallelism);

		// 将 list 结果转到 map 中
		Map<OperatorInstanceID, List<OperatorStateHandle>> result = new HashMap<>(states.size());

		for (int subtaskIndex = 0; subtaskIndex < states.size(); subtaskIndex++) {
			checkNotNull(states.get(subtaskIndex) != null, "states.get(subtaskIndex) is null");
			result.put(OperatorInstanceID.of(subtaskIndex, operatorID), states.get(subtaskIndex));
		}

		return result;
	}

	/**
	 * Repartitions the given operator state using the given {@link OperatorStateRepartitioner} with respect to the new
	 * parallelism.
	 *
	 * @param opStateRepartitioner  partitioner to use
	 * @param chainOpParallelStates state to repartition
	 * @param oldParallelism        parallelism with which the state is currently partitioned
	 * @param newParallelism        parallelism with which the state should be partitioned
	 * @return repartitioned state
	 */
	// TODO rewrite based on operator id
	public static List<List<OperatorStateHandle>> applyRepartitioner(
		OperatorStateRepartitioner opStateRepartitioner,
		List<List<OperatorStateHandle>> chainOpParallelStates,
		int oldParallelism,
		int newParallelism) {

		if (chainOpParallelStates == null) {
			return Collections.emptyList();
		}

		return opStateRepartitioner.repartitionState(
			chainOpParallelStates,
			oldParallelism,
			newParallelism);
		}

	/**
	 * Determine the subset of {@link KeyGroupsStateHandle KeyGroupsStateHandles} with correct
	 * key group index for the given subtask {@link KeyGroupRange}.
	 *
	 * <p>This is publicly visible to be used in tests.
	 */
	public static List<KeyedStateHandle> getKeyedStateHandles(
		Collection<? extends KeyedStateHandle> keyedStateHandles,
		KeyGroupRange subtaskKeyGroupRange) {

		List<KeyedStateHandle> subtaskKeyedStateHandles = new ArrayList<>(keyedStateHandles.size());

		for (KeyedStateHandle keyedStateHandle : keyedStateHandles) {
			KeyedStateHandle intersectedKeyedStateHandle = keyedStateHandle.getIntersection(subtaskKeyGroupRange);

			if (intersectedKeyedStateHandle != null) {
				subtaskKeyedStateHandles.add(intersectedKeyedStateHandle);
			}
		}

		return subtaskKeyedStateHandles;
	}
}
