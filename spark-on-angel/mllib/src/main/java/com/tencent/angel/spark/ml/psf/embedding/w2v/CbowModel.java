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
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;

public class CbowModel extends EmbeddingModel {

  public CbowModel(int dim, int negative, int window, int seed, int maxIndex, int numNodeOneRow, int maxLength, float[][] layers) {
    super(dim, negative, window, seed, maxIndex, numNodeOneRow, maxLength, layers);
  }

  @Override
  public float[] dot(int[][] sentences) {

    Random windowSeed = new Random(seed);
    Random negativeSeed = new Random(seed + 1);
    FloatArrayList partialDots = new FloatArrayList();

    IntOpenHashSet inputs = new IntOpenHashSet();
    IntOpenHashSet outputs = new IntOpenHashSet();

    float[] sentence_vectors = new float[dim * maxLength];
    float[] context = new float[dim];

    for (int s = 0; s < sentences.length; s++) {
      int[] sen = sentences[s];

      // locates the input vectors to local array to prevent randomly access
      // on the large server row.
      for (int a = 0; a < sen.length; a++) {
        int node = sen[a];
        int offset = (node % numNodeOneRow) * dim * 2;
        float[] values = layers[node / numNodeOneRow];
        int start = a * dim;
        for (int c = 0; c < dim; c++)
          sentence_vectors[start + c] = values[offset + c];
      }

      for (int position = 0; position < sen.length; position++) {
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

            int start = c * dim;
            for (c = 0; c < dim; c++) context[c] += sentence_vectors[c + start];
            inputs.add(sentence_word);
            cw++;
          }

        // Calculate the partial dot values
        if (cw > 0) {
          for (int c = 0; c < dim; c++) context[c] /= cw;
          int target;
          for (int d = 0; d < negative + 1; d++) {
            if (d == 0) target = word;
              // We should guarantee here that the sample would not equal the ``word``
            else while (true) {
              target = negativeSeed.nextInt(maxIndex);
              if (target == word) continue;
              else break;
            }

            outputs.add(target);
            float f = 0f;
            float[] values = layers[target / numNodeOneRow];
            int colId = (target % numNodeOneRow) * dim * 2 + dim;
            for (int c = 0; c < dim; c++) f += context[c] * values[c + colId];
            partialDots.add(f);
          }
        }
      }
    }

    this.numInputsToUpdate = inputs.size();
    this.numOutputsToUpdate = outputs.size();

    return partialDots.toFloatArray();
  }

  @Override
  public void adjust(int[][] sentences, ByteBuf buf, int numInputs, int numOutputs) {

    int length = buf.readInt();
    float[] sentence_vectors = new float[dim * maxLength];

    // used to accumulate the context input vectors
    float[] neu1 = new float[dim];
    float[] neu1e = new float[dim];

    float[] inputs = new float[numInputs * dim];
    float[] outputs = new float[numOutputs * dim];

    Int2IntOpenHashMap inputIndex = new Int2IntOpenHashMap();
    Int2IntOpenHashMap outputIndex = new Int2IntOpenHashMap();

    Int2IntOpenHashMap inputUpdateCounter = new Int2IntOpenHashMap();
    Int2IntOpenHashMap outputUpdateCounter = new Int2IntOpenHashMap();

    Random windowSeed = new Random(seed);
    Random negativeSeed = new Random(seed + 1);

    int[] windows = new int[window * 2];

    for (int s = 0; s < sentences.length; s++) {
      int[] sen = sentences[s];

      // locates the input vector into local arrays to prevent randomly access for
      // the large server row.
      for (int a = 0; a < sen.length; a++) {
        int node = sen[a];
        int offset = (node % numNodeOneRow) * dim * 2;
        float[] values = layers[node / numNodeOneRow];
        int start = a * dim;
        for (int c = 0; c < dim; c++)
          sentence_vectors[start + c] = values[offset + c];
      }

      for (int position = 0; position < sen.length; position++) {
        int word = sen[position];
        // window size
        int b = windowSeed.nextInt(window);
        Arrays.fill(neu1, 0);
        Arrays.fill(neu1e, 0);
        int cw = 0;

        for (int a = b; a < window * 2 + 1 - b; a++)
          if (a != window) {
            int c = position - window + a;
            if (c < 0) continue;
            if (c >= sen.length) continue;
            if (sen[c] == -1) continue;
            windows[cw] = sen[c];
            int start = c * dim;
            for (c = 0; c < dim; c++) neu1[c] += sentence_vectors[c + start];
            cw++;
          }


        if (cw > 0) {
          for (int c = 0; c < dim; c++) neu1[c] /= cw;
          int target;
          for (int d = 0; d < negative + 1; d++) {
            if (d == 0) target = word;
            else
              // while true to prevent sampling out a positive target
              while (true) {
                target = negativeSeed.nextInt(maxIndex);
                if (target == word) continue;
                else break;
              }

            float g = buf.readFloat();
            length--;
            // how to prevent the randomly access to the output vectors??
            int col = (target % numNodeOneRow) * dim * 2 + dim;
            float[] values = layers[target / numNodeOneRow];
            // accumulate gradients for the input vectors
            for (int c = 0; c < dim; c++) neu1e[c] += g * values[c + col];

            // update output vectors
            merge(outputs, outputIndex, target, neu1, g, 0);
            outputUpdateCounter.addTo(target, 1);
          }

          for (int a = 0; a < cw; a++) {
            int input = windows[a];
            merge(inputs, inputIndex, input, neu1e, 1, 0);
            inputUpdateCounter.addTo(input, 1);
          }
        }
      }
    }

    // update input
    ObjectIterator<Int2IntMap.Entry> it = inputIndex.int2IntEntrySet().fastIterator();
    while (it.hasNext()) {
      Int2IntMap.Entry entry = it.next();
      int node = entry.getIntKey();
      int offset = entry.getIntValue() * dim;
      int divider = inputUpdateCounter.get(node);
      int col = (node % numNodeOneRow) * dim * 2;
      float[] values = layers[node / numNodeOneRow];
      for (int a = 0; a < dim; a++) values[a + col] += inputs[offset + a] / divider;
    }

    // update output
    it = outputIndex.int2IntEntrySet().fastIterator();
    while (it.hasNext()) {
      Int2IntMap.Entry entry = it.next();
      int node = entry.getIntKey();
      int offset = entry.getIntValue() * dim;
      int col = (node % numNodeOneRow) * dim * 2 + dim;
      float[] values = layers[node / numNodeOneRow];
      int divider = outputUpdateCounter.get(node);
      for (int a = 0; a < dim; a++) values[a + col] += outputs[offset + a] / divider;
    }

    assert length == 0;
  }

}
