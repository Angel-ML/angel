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


package com.tencent.angel.ml.matrix.psf.update.zero;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import java.util.ArrayList;
import java.util.List;

/**
 * TODO Updates elements as zero.
 */
public class Zero extends UpdateFunc {

  /**
   * Creates a new updater.
   *
   * @param param the param
   */
  public Zero(UpdateParam param) {
    super(param);
  }

  /**
   * Creates a new updater.
   */
  public Zero() {
    this(null);
  }

  /**
   * The parameter of zero updater.
   */
  public static class ZeroParam extends UpdateParam {

    /**
     * Instantiates a new Zero updater param.
     *
     * @param matrixId the matrix id
     * @param updateClock the update clock
     */
    public ZeroParam(int matrixId, boolean updateClock) {
      super(matrixId, updateClock);
    }

    @Override
    public List<PartitionUpdateParam> split() {
      List<PartitionKey> partList =
          PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();
      List<PartitionUpdateParam> partParamList = new ArrayList<PartitionUpdateParam>(size);
      for (int i = 0; i < size; i++) {
        partParamList.add(new ZeroPartitionParam(matrixId, partList.get(i), updateClock));
      }

      return partParamList;
    }
  }


  /**
   * The partition updater parameter.
   */
  public static class ZeroPartitionParam extends PartitionUpdateParam {

    /**
     * Creates new partition updater parameter.
     *
     * @param matrixId the matrix id
     * @param partKey the part key
     * @param updateClock the update clock
     */
    public ZeroPartitionParam(int matrixId, PartitionKey partKey, boolean updateClock) {
      super(matrixId, partKey, updateClock);
    }

    /**
     * Creates a new partition updater parameter by default.
     */
    public ZeroPartitionParam() {
      super();
    }
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    RowBasedPartition part = (RowBasedPartition) psContext.getMatrixStorageManager()
        .getPart(partParam.getMatrixId(), partParam.getPartKey().getPartitionId());

    if (part != null) {
      int startRow = part.getPartitionKey().getStartRow();
      int endRow = part.getPartitionKey().getEndRow();
      for (int i = startRow; i < endRow; i++) {
        ServerRow row = part.getRow(i);
        if (row == null) {
          continue;
        }
        zero(row);
      }
    }

  }

  private void zero(ServerRow row) {
    row.startWrite();
    try {
      row.clear();
    } finally {
      row.endWrite();
    }
  }
}
