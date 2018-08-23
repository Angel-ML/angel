/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in 
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.request.UpdateItem;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.server.data.response.GetClocksResponse;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.transport.adapter.*;
import com.tencent.angel.psagent.task.TaskContext;

import java.util.List;
import java.util.concurrent.Future;

/**
 * The RPC interface between psagent and ps
 */
public interface MatrixTransportInterface {
  /**
   * Get a matrix partition.
   *
   * @param requestId user request id
   * @param partKey   partition key
   * @param clock     clock value
   * @return Future<ServerPartition> matrix partition
   */
  Future<ServerPartition> getPart(int requestId, PartitionKey partKey, int clock);

  /**
   * Get a row split.
   *
   * @param requestId user request id
   * @param partKey   partition key
   * @param rowIndex  row index
   * @param clock     clock value
   * @return Future<ServerRow> row split
   */
  Future<ServerRow> getRowSplit(int requestId, PartitionKey partKey, int rowIndex, int clock);

  /**
   * Get a batch of row splits.
   *
   * @param requestId  user request id
   * @param partKey    partition key
   * @param rowIndexes row indexes
   * @param clock      clock value
   * @return Future<List<ServerRow>> row splits
   */
  Future<List<ServerRow>> getRowsSplit(int requestId, PartitionKey partKey,
    List<Integer> rowIndexes, int clock);

  /**
   * Get the clock value of all matrix partitions that stored on the specified ps
   *
   * @param serverId ps id
   * @return Future<Map<PartitionKey, Integer>> matrix partition clocks
   */
  Future<GetClocksResponse> getClocks(ParameterServerId serverId);

  /**
   * Update matrix partition use the update udf.
   *
   * @param requestId             user request id
   * @param updateFunc            the update udf
   * @param partitionUpdaterParam parameter of the update udf
   * @return update result
   */
  Future<VoidResult> update(int requestId, UpdateFunc updateFunc,
    PartitionUpdateParam partitionUpdaterParam);

  /**
   * Update matrix partition use the update udf.
   *
   * @param updateFunc            the update udf
   * @param partitionUpdaterParam parameter of the update udf
   * @return update result
   */
  Future<VoidResult> update(UpdateFunc updateFunc, PartitionUpdateParam partitionUpdaterParam);

  /**
   * Get a partition result use the get row udf.
   *
   * @param requestId         user request id
   * @param func              the get udf
   * @param partitionGetParam parameter of the update udf
   * @return row split
   */
  Future<PartitionGetResult> get(int requestId, GetFunc func, PartitionGetParam partitionGetParam);

  /**
   * Get a partition result use the get row udf.
   *
   * @param func              the get udf
   * @param partitionGetParam parameter of the update udf
   * @return row split
   */
  Future<PartitionGetResult> get(GetFunc func, PartitionGetParam partitionGetParam);

  /**
   * Get row use indices
   *
   * @param userRequestId user request id
   * @param key           partition key
   * @param value         indices
   */
  FutureResult<IndexPartGetRowResult> indexGetRow(int userRequestId, int matrixId, int rowId,
    PartitionKey key, IndicesView value);

  FutureResult<IndexPartGetRowsResult> indexGetRows(int requestId, int matrixId,
    PartitionKey partKey, List<Integer> rowIds, IndicesView colIds);

  FutureResult<VoidResult> plus(int requestId, int matrixId, PartitionKey partKey,
    UpdateItem updateItem, TaskContext taskContext, int clock, boolean updateClock);

  FutureResult<VoidResult> update(int requestId, int matrixId, PartitionKey partKey,
    UpdateItem updateItem, TaskContext taskContext, int clock, boolean updateClock, UpdateOp op);
}
