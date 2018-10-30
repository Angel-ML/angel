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

package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.spark.ml.psf.embedding.NENegativeSample;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import it.unimi.dsi.fastutil.floats.FloatArrayList;

import java.util.Arrays;
import java.util.List;
import java.util.Random;

public class CbowDot extends GetFunc {

  public CbowDot(CbowDotParam param) {
    super(param);
  }

  public CbowDot() { super(null); }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    if (partParam instanceof CbowDotPartitionParam) {
      CbowDotPartitionParam param = (CbowDotPartitionParam) partParam;

      // some params
      PartitionKey pkey = param.getPartKey();

      int negative = param.negative;
      int partDim  = param.partDim;
      int window   = param.window;
      int seed     = param.seed;
      int order    = 2;

      // batch sentences
      int[][] sentences = ServerWrapper.getSentencesWithPartitionId(param.partitionId);
      // max index for node/word
      int maxIndex = ServerWrapper.getMaxIndex();

      // compute number of nodes for one row
      int size = (int) (pkey.getEndCol() - pkey.getStartCol());
      int numNodes = size / (partDim * order);

      // used to accumulate the context input vectors
      float[] context = new float[partDim];

      ServerPartition partition = psContext.getMatrixStorageManager().getPart(pkey);


      Random windowSeed = new Random(seed);
      Random negativeSeed = new Random(seed);
      FloatArrayList partialDots = new FloatArrayList();
      for (int s = 0; s < sentences.length; s ++) {
        int[] sen = sentences[s];
        for (int position = 0; position < sen.length; position ++) {
          int word = sen[position];
          // fill 0 for context vector
          Arrays.fill(context, 0);
          // window size
          int b = windowSeed.nextInt(window);
          // Continuous bag-of-words Models
          int cw = 0;

          // Accumulate the input vectors from context
          for (int a = b; a < window * 2 + 1 - b; a++)
            if (a != window) {
              int c = position - window + a;
              if (c < 0) continue;
              if (c >= sen.length) continue;
              int sentence_word = sen[c];
              if (sentence_word == -1) continue;
              int rowId = sentence_word / numNodes;
              int colId = (sentence_word % numNodes) * partDim * order;
              float[] values = ((IntFloatDenseVectorStorage) partition.getRow(rowId)
                .getSplit().getStorage()).getValues();
              for (c = 0; c < partDim; c++) context[c] += values[c + colId];
              cw++;
            }

          // Calculate the partial dot values
          if (cw > 0) {
            for (int c = 0; c < partDim; c ++) context[c] /= cw;
            int target;
            for (int d = 0; d < negative + 1; d ++) {
              if (d == 0) target = word;
              // We should guarantee here that the sample would not equal the ``word``
              else target = negativeSeed.nextInt(maxIndex);

              int rowId = target / numNodes;
              int colId = (target % numNodes) * partDim * order + partDim;
              float f = 0f;
              float[] values = ((IntFloatDenseVectorStorage) partition.getRow(rowId)
                .getSplit().getStorage()).getValues();
              for (int c = 0; c < partDim; c ++) f += context[c] * values[c + colId];
              partialDots.add(f);
            }
          }
        }
      }
      return new CbowDotPartitionResult(partialDots.toFloatArray());
    }

    return null;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    if (partResults.size() > 0 && partResults.get(0) instanceof CbowDotPartitionResult) {
      int size = ((CbowDotPartitionResult) partResults.get(0)).length;

      // check the length of dot values
      for (PartitionGetResult result: partResults) {
        if (result instanceof CbowDotPartitionResult &&
          size != ((CbowDotPartitionResult) result).length)
          throw new AngelException(
                  String.format("length of dot values not same one is %d other is %d",
                          size,
                          ((CbowDotPartitionResult) result).length));
      }

      // merge dot values from all partitions
      float[] results = new float[size];
      for (PartitionGetResult result: partResults)
        if (result instanceof CbowDotPartitionResult)
          try {
            ((CbowDotPartitionResult) result).merge(results);
          } finally {
            ((CbowDotPartitionResult) result).clear();
          }
      return new CbowDotResult(results);
    }

    return null;
  }
}
