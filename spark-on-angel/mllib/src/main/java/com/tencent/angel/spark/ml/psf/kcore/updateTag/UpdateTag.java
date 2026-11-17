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

package com.tencent.angel.spark.ml.psf.kcore.updateTag;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongIntRow;

/**
 * Init node neighbors for long type node id
 */
public class UpdateTag extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */

  public UpdateTag(int matrixId, long[] nodes, int rowId) {
    this(new UpdateTagParam(matrixId, nodes, rowId));
  }

  public UpdateTag(UpdateTagParam param) {
    super(param);
  }

  public UpdateTag() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartUpdateTagParam param = (PartUpdateTagParam) partParam;
    ServerPartition part = psContext.getMatrixStorageManager().getPart(partParam.getPartKey());
    ServerLongIntRow row = (ServerLongIntRow) (((RowBasedPartition) part).getRow(param.getRowId()));

    long[] nodeId = param.getNodeIds();

    int i = 0;
    while (i < nodeId.length) {
      row.set(nodeId[i], 1);
      i += 1;
    }
  }
}
