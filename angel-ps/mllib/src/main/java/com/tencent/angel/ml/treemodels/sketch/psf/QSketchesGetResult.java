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

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.psagent.matrix.ResponseType;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public class QSketchesGetResult extends GetResult {
  private Map<Integer, float[]> quantiles;

  public QSketchesGetResult(ResponseType type, Map<Integer, float[]> quantiles) {
    super(type);
    this.quantiles = quantiles;
  }

  public float[] getQuantiles(int index) {
    if (quantiles.containsKey(index))
      return quantiles.get(index);
    else
      return null;
  }

  public static class QSketchesPartitionGetResult extends PartitionGetResult {
    protected int numQuantiles;
    protected int[] rowIndexes;
    protected float[][] quantiles;

    public QSketchesPartitionGetResult(int numQuantiles, int[] rowIndexes, float[][] quantiles) {
      this.numQuantiles = numQuantiles;
      this.rowIndexes = rowIndexes;
      this.quantiles = quantiles;
    }

    public QSketchesPartitionGetResult() {
      this.numQuantiles = -1;
      this.rowIndexes = null;
      this.quantiles = null;
    }

    public int getNumQuantiles() {
      return numQuantiles;
    }

    /**
     * Serialize object to the Netty ByteBuf.
     *
     * @param buf the Netty ByteBuf
     */
    @Override public void serialize(ByteBuf buf) {
      buf.writeInt(numQuantiles);
      buf.writeInt(rowIndexes.length);
      for (int i = 0; i < rowIndexes.length; i++) {
        buf.writeInt(rowIndexes[i]);
        for (float q : quantiles[i])
          buf.writeFloat(q);
      }
    }

    /**
     * Deserialize object from the Netty ByteBuf.
     *
     * @param buf the Netty ByteBuf
     */
    @Override public void deserialize(ByteBuf buf) {
      this.numQuantiles = buf.readInt();
      int nrows = buf.readInt();
      this.rowIndexes = new int[nrows];
      this.quantiles = new float[nrows][this.numQuantiles];
      for (int i = 0; i < nrows; i++) {
        this.rowIndexes[i] = buf.readInt();
        for (int j = 0; j < this.numQuantiles; j++)
          quantiles[i][j] = buf.readFloat();
      }
    }

    /**
     * Estimate serialized data size of the object, it used to ByteBuf allocation.
     *
     * @return int serialized data size of the object
     */
    @Override public int bufferLen() {
      return 8 + rowIndexes.length * (4 + 4 * numQuantiles);
    }
  }
}
