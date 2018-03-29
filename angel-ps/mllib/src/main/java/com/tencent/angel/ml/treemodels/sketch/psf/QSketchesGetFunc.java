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
 *
 */

package com.tencent.angel.ml.treemodels.sketch.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ml.utils.Maths;
import com.tencent.angel.ps.impl.matrix.ServerDenseFloatRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.ResponseType;
import io.netty.buffer.ByteBuf;

import java.nio.FloatBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QSketchesGetFunc extends GetFunc {
  public QSketchesGetFunc(QSketchesGetParam param) {
    super(param);
  }

  public QSketchesGetFunc(int matrixId, int[] rowIndexes, int numWorker, int numQuantile) {
    this(new QSketchesGetParam(matrixId, rowIndexes, numWorker, numQuantile));
  }

  public QSketchesGetFunc() {
    super(null);
  }

  public static class QSketchesGetParam extends GetParam {
    protected int[] rowIndexes;
    protected int numWorker;
    protected int numQuantile;

    public QSketchesGetParam(int matrixId, int[] rowIndexes, int numWorker, int numQuantile) {
      super(matrixId);
      this.rowIndexes = rowIndexes;
      this.numWorker = numWorker;
      this.numQuantile = numQuantile;
    }

    @Override public List<PartitionGetParam> split() {
      List<PartitionKey> partList =
        PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
      int size = partList.size();

      List<PartitionGetParam> partParams = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        PartitionKey partKey = partList.get(i);
        List<Integer> partRowIndexes = new ArrayList<>();
        for (int j = 0; j < rowIndexes.length; j++) {
          if (partKey.getStartRow() <= rowIndexes[j] && partKey.getEndRow() > rowIndexes[j]) {
            partRowIndexes.add(rowIndexes[j]);
          }
        }
        if (partRowIndexes.size() > 0) {
          partParams.add(
            new QSketchesPartitionGetParam(matrixId, partKey, Maths.intList2Arr(partRowIndexes),
              numWorker, numQuantile));
        }
      }
      return partParams;
    }
  }


  public static class QSketchesPartitionGetParam extends PartitionGetParam {
    protected int[] rowIndexes;
    protected int numWorker;
    protected int numQuantile;

    public QSketchesPartitionGetParam(int matrixId, PartitionKey partKey, int[] rowIndexes,
      int numWorker, int numQuantile) {
      super(matrixId, partKey);
      this.rowIndexes = rowIndexes;
      this.numWorker = numWorker;
      this.numQuantile = numQuantile;
    }

    public QSketchesPartitionGetParam() {
      super(-1, null);
      this.rowIndexes = null;
      this.numWorker = -1;
      this.numQuantile = -1;
    }

    @Override public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(rowIndexes.length);
      for (int rid : rowIndexes)
        buf.writeInt(rid);
      buf.writeInt(numWorker);
      buf.writeInt(numQuantile);
    }

    @Override public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      int nrows = buf.readInt();
      this.rowIndexes = new int[nrows];
      for (int i = 0; i < nrows; i++)
        this.rowIndexes[i] = buf.readInt();
      this.numWorker = buf.readInt();
      this.numQuantile = buf.readInt();
    }

    @Override public int bufferLen() {
      return super.bufferLen() + 12 + rowIndexes.length * 4;
    }
  }

  /**
   * Partition get. This function is called on PS.
   *
   * @param partParam the partition parameter
   * @return the partition result
   */
  @Override public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    QSketchesPartitionGetParam param = (QSketchesPartitionGetParam) partParam;
    List<Integer> rowIndexes = new ArrayList<>(param.rowIndexes.length);
    List<float[]> quantiles = new ArrayList<>(param.rowIndexes.length);
    for (int rowId : param.rowIndexes) {
      ServerDenseFloatRow row = (ServerDenseFloatRow) psContext.getMatrixStorageManager()
        .getRow(param.getMatrixId(), rowId, param.getPartKey().getPartitionId());

      FloatBuffer buf = row.getData();
      int numMerged = Float.floatToIntBits(buf.get(0));
      if (numMerged == param.numWorker) {
        float[] q = new float[param.numQuantile];
        for (int i = 0; i < param.numQuantile; i++)
          q[i] = buf.get(i + 1);
        rowIndexes.add(rowId);
        quantiles.add(q);
      }
    }
    return new QSketchesGetResult.QSketchesPartitionGetResult(param.numQuantile,
      Maths.intList2Arr(rowIndexes),
      quantiles.toArray(new float[quantiles.size()][param.numQuantile]));
  }

  /**
   * Merge the partition get results. This function is called on PSAgent.
   *
   * @param partResults the partition results
   * @return the merged result
   */
  @Override public GetResult merge(List<PartitionGetResult> partResults) {
    int nrows = 0;
    for (int i = 0; i < partResults.size(); i++)
      nrows +=
        ((QSketchesGetResult.QSketchesPartitionGetResult) partResults.get(i)).rowIndexes.length;
    Map<Integer, float[]> quantilesMap = new HashMap<>(nrows);
    for (int i = 0; i < partResults.size(); i++) {
      QSketchesGetResult.QSketchesPartitionGetResult partResult =
        (QSketchesGetResult.QSketchesPartitionGetResult) partResults.get(i);
      int[] rowIndexes = partResult.rowIndexes;
      float[][] quantiles = partResult.quantiles;
      for (int j = 0; j < rowIndexes.length; j++)
        quantilesMap.put(rowIndexes[j], quantiles[j]);
    }
    return new QSketchesGetResult(ResponseType.SUCCESS, quantilesMap);
  }
}
