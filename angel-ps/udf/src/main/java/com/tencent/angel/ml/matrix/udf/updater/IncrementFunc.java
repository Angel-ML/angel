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

package com.tencent.angel.ml.matrix.udf.updater;

import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.matrix.oplog.cache.MatrixOpLog;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

/**
 * The function of Increment.
 */
public class IncrementFunc extends DefaultUpdaterFunc{

  /**
   * Creates a new function.
   *
   * @param param the param
   */
  public IncrementFunc(IncrementUpdaterParam param) {
    super(param);
  }

  /**
   * The parameter of partition increment.
   */
  class IncrementPartitionUpdaterParam extends PartitionUpdaterParam{
    private List<RowUpdateSplit> rowUpdateSplits;

    /**
     * Creates a new parameter.
     *
     * @param matrixId        the matrix id
     * @param partKey         the part key
     * @param updateClock     the update clock
     * @param rowUpdateSplits the row update splits
     */
    public IncrementPartitionUpdaterParam(
        int matrixId, PartitionKey partKey, boolean updateClock,
        List<RowUpdateSplit> rowUpdateSplits) {
      super(matrixId, partKey, updateClock);
      this.setRowUpdateSplits(rowUpdateSplits);
    }

    /**
     * Creates a new parameter by default.
     */
    public IncrementPartitionUpdaterParam() {
      super();
      this.setRowUpdateSplits(null);
    }


    @Override
    public void serialize(ByteBuf buf) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public void deserialize(ByteBuf buf) {
      // TODO Auto-generated method stub
      
    }

    @Override
    public int bufferLen() {
      // TODO Auto-generated method stub
      return 0;
    }

    /**
     * Gets row update splits.
     *
     * @return the row update splits
     */
    public List<RowUpdateSplit> getRowUpdateSplits() {
      return rowUpdateSplits;
    }

    /**
     * Sets row update splits.
     *
     * @param rowUpdateSplits the row update splits
     */
    public void setRowUpdateSplits(List<RowUpdateSplit> rowUpdateSplits) {
      this.rowUpdateSplits = rowUpdateSplits;
    }

  }

  /**
   * The parameter of increment updater.
   */
  public class IncrementUpdaterParam extends UpdaterParam {
    private final MatrixOpLog matrixOpLog;

    /**
     * Instantiates a new Increment updater param.
     *
     * @param matrixId    the matrix id
     * @param updateClock the update clock
     * @param matrixOpLog the matrix op log
     */
    public IncrementUpdaterParam(int matrixId, boolean updateClock, MatrixOpLog matrixOpLog) {
      super(matrixId, updateClock);
      this.matrixOpLog = matrixOpLog;
    }

    @Override
    public List<PartitionUpdaterParam> split() {
      List<PartitionUpdaterParam> partitionParams = new ArrayList<PartitionUpdaterParam>();
      Map<ParameterServerId, Map<PartitionKey, List<RowUpdateSplit>>> psUpdateData =
          new HashMap<ParameterServerId, Map<PartitionKey, List<RowUpdateSplit>>>();
      matrixOpLog.split(psUpdateData);
      for (Map<PartitionKey, List<RowUpdateSplit>> partToSplitsMap : psUpdateData.values()) {
        partToSplitsMap.entrySet();
        for (Entry<PartitionKey, List<RowUpdateSplit>> partSplits : partToSplitsMap.entrySet()) {
          partitionParams.add(new IncrementPartitionUpdaterParam(matrixId, partSplits.getKey(),
              updateClock, partSplits.getValue()));
        }
      }

      return partitionParams;
    }
  }
  
  @Override
  public void partitionUpdate(PartitionUpdaterParam partParam) {
    // TODO Auto-generated method stub
    
  }
}
