/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.calcite.bridge;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.catalog.ResolvedSchema;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.OperationUtils;
import org.apache.flink.table.operations.QueryOperation;
import org.apache.flink.table.operations.QueryOperationVisitor;

import org.apache.calcite.rel.RelNode;

import java.util.Collections;
import java.util.List;

/**
 * Wrapper for valid logical plans and resolved schema generated by Planner. It's mainly used by
 * pluggable dialect which will generate Calcite RelNode in planning phase.
 */
@Internal
public class PlannerExternalQueryOperation implements QueryOperation {

    private final RelNode calciteTree;
    private final ResolvedSchema resolvedSchema;

    public PlannerExternalQueryOperation(RelNode relNode, ResolvedSchema resolvedSchema) {
        this.calciteTree = relNode;
        this.resolvedSchema = resolvedSchema;
    }

    public RelNode getCalciteTree() {
        return calciteTree;
    }

    @Override
    public ResolvedSchema getResolvedSchema() {
        return resolvedSchema;
    }

    @Override
    public List<QueryOperation> getChildren() {
        return Collections.emptyList();
    }

    @Override
    public <T> T accept(QueryOperationVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String asSummaryString() {
        return OperationUtils.formatWithChildren(
                "PlannerCalciteQueryOperation",
                Collections.emptyMap(),
                getChildren(),
                Operation::asSummaryString);
    }
}