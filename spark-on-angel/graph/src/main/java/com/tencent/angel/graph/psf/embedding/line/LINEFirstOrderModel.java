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

package com.tencent.angel.graph.psf.embedding.line;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;

public class LINEFirstOrderModel extends EmbeddingModel {


  public LINEFirstOrderModel(int dim, int negative, int seed, int maxIndex, int numNodeOneRow, float[][] layers) {
    super(dim, negative, seed, maxIndex, numNodeOneRow, layers);
  }

  @Override
  public float[] dot(ByteBuf edges) {

    Random negativeSeed = new Random(seed);
    IntOpenHashSet numInputs = new IntOpenHashSet();

    int batchSize = edges.readInt();
    float[] partialDots = new float[batchSize * (1 + negative)];
    int dotInc = 0;
    for (int position = 0; position < batchSize; position++) {
      int src = edges.readInt();
      int dst = edges.readInt();
      // Skip-Gram model
      float[] inputs = layers[src / numNodeOneRow];
      int l1 = (src % numNodeOneRow) * dim;
      numInputs.add(src);

      // Negative sampling
      int target;
      for (int a = 0; a < negative + 1; a++) {
        if (a == 0) target = dst;
        else do {
          target = negativeSeed.nextInt(maxIndex);
        } while (target == src);

        numInputs.add(target);
        float[] outputs = layers[target / numNodeOneRow];
        int l2 = (target % numNodeOneRow) * dim;
        float f = 0.0f;
        for (int b = 0; b < dim; b++) f += inputs[l1 + b] * outputs[l2 + b];
        partialDots[dotInc++] = f;
      }
    }
    this.numInputsToUpdate = numInputs.size();
    return partialDots;
  }

  @Override
  public void adjust(ByteBuf dataBuf, int numInputs, int numOutputs) {

    // used to accumulate the updates for input vectors
    float[] neu1e = new float[dim];
    float[] inputUpdates = new float[numInputs * dim];
    Int2IntOpenHashMap inputIndex = new Int2IntOpenHashMap();
    Int2IntOpenHashMap inputUpdateCounter = new Int2IntOpenHashMap();
    Random negativeSeed = new Random(seed);
    int batchSize = dataBuf.readInt();

    for (int position = 0; position < batchSize; position++) {
      int src = dataBuf.readInt();
      int dst = dataBuf.readInt();

      float[] inputs = layers[src / numNodeOneRow];
      int l1 = (src % numNodeOneRow) * dim;

      Arrays.fill(neu1e, 0);

      // Negative sampling
      int target;
      for (int d = 0; d < negative + 1; d++) {
        if (d == 0) target = dst;
        else do {
          target = negativeSeed.nextInt(maxIndex);
        } while (target == src);

        float[] outputs = layers[target / numNodeOneRow];
        int l2 = (target % numNodeOneRow) * dim;

        float g = dataBuf.readFloat();

        // accumulate for the hidden layer
        for (int a = 0; a < dim; a++) neu1e[a] += g * outputs[a + l2];
        // update output layer
        merge(inputUpdates, inputIndex, target, inputs, g, l1);
        inputUpdateCounter.addTo(target, 1);
      }

      // update the hidden layer
      merge(inputUpdates, inputIndex, src, neu1e, 1, 0);
      inputUpdateCounter.addTo(src, 1);
    }

    // update input
    ObjectIterator<Int2IntMap.Entry> it = inputIndex.int2IntEntrySet().fastIterator();
    while (it.hasNext()) {
      Int2IntMap.Entry entry = it.next();
      int node = entry.getIntKey();
      int offset = entry.getIntValue() * dim;
      int divider = inputUpdateCounter.get(node);
      int col = (node % numNodeOneRow) * dim;
      float[] values = layers[node / numNodeOneRow];
      for (int a = 0; a < dim; a++) values[a + col] += inputUpdates[offset + a] / divider;
    }
  }
}
