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

package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import com.tencent.angel.spark.ml.psf.embedding.NEDot;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class Dot extends GetFunc {

  public Dot(DotParam param) {
    super(param);
  }

  public Dot() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    if (partParam instanceof DotPartitionParam) {

      DotPartitionParam param = (DotPartitionParam) partParam;

      float[] dots;
      try {
        // some params
        PartitionKey pkey = param.getPartKey();

        int seed = param.seed;
        ByteBuf edges = param.edgeBuf;

        // max index for node/word
        int maxIndex = ServerWrapper.getMaxIndex();
        int negative = ServerWrapper.getNegative();
        int partDim = ServerWrapper.getPartDim();
        int order = ServerWrapper.getOrder();

        // compute number of nodes for one row
        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
        int numNodes = size / (partDim * order);

        int numRows = pkey.getEndRow() - pkey.getStartRow();
        float[][] layers = new float[numRows][];
        for (int row = 0; row < numRows; row++)
          layers[row] = ServerRowUtils.getVector((ServerIntFloatRow) psContext.getMatrixStorageManager()
              .getRow(pkey, row)).getStorage()
              .getValues();

        EmbeddingModel model;
        switch (order) {
          case 1:
            model = new LINEFirstOrderModel(partDim, negative, seed, maxIndex, numNodes, layers);
            break;
          default:
            model = new LINESecondOrderModel(partDim, negative, seed, maxIndex, numNodes, layers);
        }
        dots = model.dot(edges);
        ServerWrapper.setNumInputs(param.partitionId, model.getNumInputsToUpdate());
        ServerWrapper.setNumOutputs(param.partitionId, model.getNumOutputsToUpdate());
      } finally {
        param.clear();
      }

      return new DotPartitionResult(dots);
    }

    return null;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    if (partResults.size() > 0 && partResults.get(0) instanceof DotPartitionResult) {
      int size = ((DotPartitionResult) partResults.get(0)).length;

      // check the length of dot values
      for (PartitionGetResult result : partResults) {
        if (result instanceof DotPartitionResult &&
            size != ((DotPartitionResult) result).length)
          throw new AngelException(
              String.format("length of dot values not same one is %d other is %d",
                  size,
                  ((DotPartitionResult) result).length));
      }

      // merge dot values from all partitions
      float[] results = new float[size];
      for (PartitionGetResult result : partResults)
        if (result instanceof DotPartitionResult)
          try {
            ((DotPartitionResult) result).merge(results);
          } finally {
            ((DotPartitionResult) result).clear();
          }
      return new NEDot.NEDotResult(results);
    }

    return null;
  }
}
