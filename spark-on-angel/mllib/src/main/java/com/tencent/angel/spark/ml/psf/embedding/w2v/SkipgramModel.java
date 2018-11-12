package com.tencent.angel.spark.ml.psf.embedding.w2v;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.floats.FloatArrayList;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;

public class SkipgramModel extends EmbeddingModel {


  public SkipgramModel(int dim, int negative, int window, int seed, int maxIndex, int numNodeOneRow, int maxLength, float[][] layers) {
    super(dim, negative, window, seed, maxIndex, numNodeOneRow, maxLength, layers);
  }

  @Override
  public float[] dot(int[][] sentences) {

    Random windowSeed = new Random(seed);
    Random negativeSeed = new Random(seed + 1);
    FloatArrayList partialDots = new FloatArrayList();

    IntOpenHashSet numInputs = new IntOpenHashSet();
    IntOpenHashSet numOutputs = new IntOpenHashSet();


    for (int s = 0; s < sentences.length; s++) {
      int[] sen = sentences[s];

      for (int position = 0; position < sen.length; position++) {
        int word = sen[position];
        // window size
        int b = windowSeed.nextInt(window);
        // Skip-Gram model

        float[] inputs = layers[word / numNodeOneRow];
        int l1 = (word % numNodeOneRow) * dim * 2;

        numInputs.add(word);

        // Accumulate the input vectors from context
        for (int a = b; a < window * 2 + 1 - b; a++)
          if (a != window) {
            int c = position - window + a;
            if (c < 0) continue;
            if (c >= sen.length) continue;
            int sentence_word = sen[c];
            if (sentence_word == -1) continue;

            // Negative sampling

            int target;
            for (int d = 0; d < negative + 1; d ++) {
              if (d == 0) target = word;
              else do{
                target = negativeSeed.nextInt(maxIndex);
              }while (target == word);

              numOutputs.add(target);

              float[] outputs = layers[target / numNodeOneRow];
              int l2 = (target % numNodeOneRow) * dim * 2 + dim;
              float f = 0.0f;
              for (c = 0; c < dim; c++) f += inputs[l1 + c] * outputs[l2 + c];
              partialDots.add(f);
            }
          }
      }
    }

    this.numInputsToUpdate = numInputs.size();
    this.numOutputsToUpdate = numOutputs.size();

    return partialDots.toFloatArray();
  }

  @Override
  public void adjust(int[][] sentences, ByteBuf buf, int numInputs, int numOutputs) {

    int length = buf.readInt();

    // used to accumulate the updates for input vectors
    float[] neu1e = new float[dim];

    float[] inputUpdates  = new float[numInputs * dim];
    float[] outputUpdates = new float[numOutputs * dim];

    Int2IntOpenHashMap inputIndex = new Int2IntOpenHashMap();
    Int2IntOpenHashMap outputIndex = new Int2IntOpenHashMap();

    Int2IntOpenHashMap inputUpdateCounter = new Int2IntOpenHashMap();
    Int2IntOpenHashMap outputUpdateCounter = new Int2IntOpenHashMap();

    Random windowSeed = new Random(seed);
    Random negativeSeed = new Random(seed + 1);

    for (int s = 0; s < sentences.length; s++) {
      int[] sen = sentences[s];

      for (int position = 0; position < sen.length; position++) {
        int word = sen[position];

        float[] inputs = layers[word / numNodeOneRow];
        int l1 = (word % numNodeOneRow) * dim * 2;

        // window size
        int b = windowSeed.nextInt(window);
        Arrays.fill(neu1e, 0);

        // skip-gram model
        for (int a = b; a < window * 2 + 1 - b; a++)
          if (a != window) {
            int c = position - window + a;
            if (c < 0) continue;
            if (c >= sen.length) continue;
            if (sen[c] == -1) continue;

            // Negative sampling

            int target;
            for (int d = 0; d < negative + 1; d ++) {
              if (d == 0) target = word;
              else while (true) {
                target = negativeSeed.nextInt(maxIndex);
                if (target == word) continue;
                else break;
              }


              float[] outputs = layers[target / numNodeOneRow];
              int l2 = (target % numNodeOneRow) * dim * 2 + dim;

              float g = buf.readFloat();

              // accumulate for the hidden layer
              for (c = 0; c < dim; c ++) neu1e[c] += g * outputs[c + l2];
              // update output layer
              merge(outputUpdates, outputIndex, target, inputs, g, l1);
              outputUpdateCounter.addTo(target, 1);
            }

            // update the hidden layer
            merge(inputUpdates, inputIndex, word, neu1e, 1, 0);
            inputUpdateCounter.addTo(word, 1);
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
      for (int a = 0; a < dim; a++) values[a + col] += inputUpdates[offset + a] / divider;
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
      for (int a = 0; a < dim; a++) values[a + col] += outputUpdates[offset + a] / divider;
    }

    assert length == 0;
  }
}
