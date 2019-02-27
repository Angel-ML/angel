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

package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.get.base.*;

import java.util.List;

public class W2VPull extends GetFunc {

  public W2VPull(GetParam param) {
    super(param);
  }

  public W2VPull() { super(null);}

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {

    if (partParam instanceof W2VPullParatitionParam) {
      W2VPullParatitionParam param = (W2VPullParatitionParam) partParam;
      int[] indices = param.indices;
      int numNodePerRow = param.numNodePerRow;

      PartitionKey pkey = param.getPartKey();
      int startRow = pkey.getStartRow();
      int startNode = startRow * numNodePerRow;
      int dimension = param.dimension;

      float[] result = new float[indices.length * dimension * 2];
      int idx = 0;

      int totalRows = pkey.getEndRow() - startRow;
      float[][] rows = new float[totalRows][];

      for (int row = startRow; row < startRow + totalRows; row ++)
        rows[row - startRow] = ((IntFloatDenseVectorStorage) psContext.getMatrixStorageManager()
          .getRow(pkey, row).getSplit().getStorage())
          .getValues();

      for (int a = 0; a < indices.length; a ++) {
        int node = indices[a];
        int rowId  = (node - startNode) / numNodePerRow;
        int offset  = (node % numNodePerRow) * dimension * 2;
        float[] layer = rows[rowId];
        for (int b = 0; b < dimension * 2; b ++) result[idx++] = layer[offset + b];
      }

      return new W2VPullPartitionResult(param.start,
        dimension,
        result);
    }

    return null;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {

    if (this.param instanceof W2VPullParam) {
      W2VPullParam param = (W2VPullParam) this.param;
      int[] indices = param.indices;
      int dimension = param.dimension;
      float[] layers = new float[indices.length * dimension * 2];

      int total = 0;
      for (PartitionGetResult r: partResults) {
        total += ((W2VPullPartitionResult)r).length;
      }
      assert total == (indices.length * dimension * 2);

      for (PartitionGetResult r: partResults) {
        W2VPullPartitionResult result = (W2VPullPartitionResult) r;

        try {
          result.merge(layers);
        } finally {
          result.clear();
        }
      }
      return new W2VPullResult(indices, layers);
    }
    return null;
  }
}
