/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.api.operators;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

/**
 * A {@link StreamOperator} for executing {@link FlatMapFunction FlatMapFunctions}.
 */
@Internal
public class StreamFlatMap<IN, OUT>
		// 继承 AbstractUdfStreamOperator，F 的泛型为 FlatMapFunction
		extends AbstractUdfStreamOperator<OUT, FlatMapFunction<IN, OUT>>
		implements OneInputStreamOperator<IN, OUT> {

	private static final long serialVersionUID = 1L;

	private transient TimestampedCollector<OUT> collector;

	public StreamFlatMap(FlatMapFunction<IN, OUT> flatMapper) {
		// super 表示将 FlatMapFunction 传递给 AbstractUdfStreamOperator
		// 将 FlatMapFunction 维护在 AbstractUdfStreamOperator 的 userFunction 中
		super(flatMapper);
		chainingStrategy = ChainingStrategy.ALWAYS;
	}

	@Override
	public void open() throws Exception {
		super.open();
		collector = new TimestampedCollector<>(output);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		collector.setTimestamp(element);
		// 处理数据时，调用的是 userFunction 的 flatMap 方法，
		// 即：用户在 flatMap 中定义的业务逻辑
		userFunction.flatMap(element.getValue(), collector);
	}
}
