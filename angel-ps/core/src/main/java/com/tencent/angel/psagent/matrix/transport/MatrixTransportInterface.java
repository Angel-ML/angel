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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.impl.matrix.ServerPartition;
import com.tencent.angel.ps.impl.matrix.ServerRow;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;

/**
 * The RPC interface between psagent and ps
 */
public interface MatrixTransportInterface {
  /**
   * Get a matrix partition.
   * 
   * @param partKey partition key
   * @param clock clock value
   * @return Future<ServerPartition> matrix partition
   */
  Future<ServerPartition> getPart(PartitionKey partKey, int clock);

  /**
   * Get a row split.
   * 
   * @param partKey partition key
   * @param rowIndex row index
   * @param clock clock value
   * @return Future<ServerRow> row split
   */
  Future<ServerRow> getRowSplit(PartitionKey partKey, int rowIndex, int clock);

  /**
   * Get a batch of row splits.
   * 
   * @param partKey partition key
   * @param rowIndexes row indexes
   * @param clock clock value
   * @return Future<List<ServerRow>> row splits
   */
  Future<List<ServerRow>> getRowsSplit(PartitionKey partKey, List<Integer> rowIndexes, int clock);

  /**
   * Get the clock value of all matrix partitions that stored on the specified ps
   * 
   * @param serverId ps id
   * @return Future<Map<PartitionKey, Integer>> matrix partition clocks
   */
  Future<Map<PartitionKey, Integer>> getClocks(ParameterServerId serverId);

  /**
   * Update a matrix partition.
   * 
   * @param partKey partition key
   * @param rowsSplit the matrix partition update splits
   * @param taskIndex task index
   * @param clock clock value
   * @param updateClock true means update clock value for the partition
   * @return Future<VoidResult> update result
   */
  Future<VoidResult> putPart(PartitionKey partKey, List<RowUpdateSplit> rowsSplit, int taskIndex,
      int clock, boolean updateClock);


  /**
   * Update matrix partition use the update udf.
   * 
   * @param updateFunc the update udf
   * @param partitionUpdaterParam parameter of the update udf
   * @return update result
   */
  Future<VoidResult> update(UpdateFunc updateFunc, PartitionUpdateParam partitionUpdaterParam);

  /**
   * Get a partition result use the get row udf.
   * 
   * @param func the get udf
   * @param partitionGetParam parameter of the update udf
   * @return row split
   */
  Future<PartitionGetResult> get(GetFunc func, PartitionGetParam partitionGetParam);
}
