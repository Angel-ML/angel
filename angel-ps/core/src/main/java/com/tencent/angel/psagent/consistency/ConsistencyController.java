/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.psagent.consistency;

import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowFunc;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.udf.updater.VoidResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;
import com.tencent.angel.psagent.task.TaskContext;

import java.util.concurrent.Future;

public interface ConsistencyController {
  void init();

  TVector getRow(TaskContext taskContext, int matrixId, int rowIndex) throws Exception;

  GetRowsResult getRowsFlow(TaskContext taskContext, RowIndex rowIndex, int rpcBatchSize)
      throws Exception;

  Future<VoidResult> clock(TaskContext taskContext, int matrixId, boolean flushFirst);

  GetRowResult getRow(TaskContext taskContext, GetRowFunc func) throws Exception;
}
