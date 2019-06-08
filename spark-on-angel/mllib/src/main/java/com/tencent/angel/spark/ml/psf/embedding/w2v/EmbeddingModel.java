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

package com.tencent.angel.spark.ml.psf.embedding.w2v;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

abstract class EmbeddingModel {

  protected int dim;
  protected int negative;
  protected int window;
  protected int seed;
  protected int maxIndex;

  protected int numNodeOneRow;
  protected int maxLength;

  protected float[][] layers;

  protected int numInputsToUpdate;
  protected int numOutputsToUpdate;

  public EmbeddingModel(int dim, int negative, int window, int seed, int maxIndex, int numNodeOneRow, int maxLength, float[][] layers) {
    this.dim = dim;
    this.negative = negative;
    this.window = window;
    this.seed = seed;
    this.maxIndex = maxIndex;
    this.numNodeOneRow = numNodeOneRow;
    this.maxLength = maxLength;
    this.layers = layers;
  }

  abstract public float[] dot(int[][] sentences);

  abstract public void adjust(int[][] sentences, ByteBuf buf, int numInput, int numOutput);

  protected void merge(float[] inputs, Int2IntOpenHashMap inputIndex, int node, float[] update, float g, int idx) {
    int start = inputIndex.get(node);
    if (!inputIndex.containsKey(node)) {
      start = inputIndex.size();
      inputIndex.put(node, start);
    }

    int offset = start * dim;
    for (int c = 0; c < dim; c++) inputs[offset + c] += g * update[c + idx];
  }

  public int getNumInputsToUpdate() {
    return numInputsToUpdate;
  }

  public int getNumOutputsToUpdate() {
    return numOutputsToUpdate;
  }
}
