package com.tencent.angel.spark.ml.psf.embedding.line;

import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;

public class LINESecondOrderModel extends EmbeddingModel {


  public LINESecondOrderModel(int dim, int negative, int seed, int maxIndex, int numNodeOneRow, float[][] layers) {
    super(dim, negative, seed, maxIndex, numNodeOneRow, layers);
  }

  @Override
  public float[] dot(ByteBuf edges) {

    Random negativeSeed = new Random(seed);
    IntOpenHashSet numInputs = new IntOpenHashSet();
    IntOpenHashSet numOutputs = new IntOpenHashSet();

    int batchSize = edges.readInt();
    float[] partialDots = new float[batchSize * (1 + negative)];
    int dotInc = 0;
    for (int position = 0; position < batchSize; position++) {
      int src = edges.readInt();
      // Skip-Gram model
      float[] inputs = layers[src / numNodeOneRow];
      int l1 = (src % numNodeOneRow) * dim * 2;
      numInputs.add(src);

      // Negative sampling
      int target;
      for (int a = 0; a < negative + 1; a++) {
        if (a == 0) target = edges.readInt();
        else do {
          target = negativeSeed.nextInt(maxIndex);
        } while (target == src);

        numOutputs.add(target);
        float[] outputs = layers[target / numNodeOneRow];
        int l2 = (target % numNodeOneRow) * dim * 2 + dim;
        float f = 0.0f;
        for (int b = 0; b < dim; b++) f += inputs[l1 + b] * outputs[l2 + b];
        partialDots[dotInc++] = f;
      }
    }

    this.numInputsToUpdate = numInputs.size();
    this.numOutputsToUpdate = numOutputs.size();

    return partialDots;
  }

  @Override
  public void adjust(ByteBuf dataBuf, int numInputs, int numOutputs) {

    // used to accumulate the updates for input vectors
    float[] neu1e = new float[dim];

    float[] inputUpdates = new float[numInputs * dim];
    float[] outputUpdates = new float[numOutputs * dim];

    Int2IntOpenHashMap inputIndex = new Int2IntOpenHashMap();
    Int2IntOpenHashMap outputIndex = new Int2IntOpenHashMap();

    Int2IntOpenHashMap inputUpdateCounter = new Int2IntOpenHashMap();
    Int2IntOpenHashMap outputUpdateCounter = new Int2IntOpenHashMap();

    Random negativeSeed = new Random(seed);
    int batchSize = dataBuf.readInt();

    for (int position = 0; position < batchSize; position++) {
      int src = dataBuf.readInt();

      float[] inputs = layers[src / numNodeOneRow];
      int l1 = (src % numNodeOneRow) * dim * 2;

      Arrays.fill(neu1e, 0);

      // Negative sampling
      int target;
      for (int d = 0; d < negative + 1; d++) {
        if (d == 0) target = dataBuf.readInt();
        else do {
          target = negativeSeed.nextInt(maxIndex);
        } while (target == src);

        float[] outputs = layers[target / numNodeOneRow];
        int l2 = (target % numNodeOneRow) * dim * 2 + dim;

        float g = dataBuf.readFloat();

        // accumulate for the hidden layer
        for (int a = 0; a < dim; a++) neu1e[a] += g * outputs[a + l2];
        // update output layer
        merge(outputUpdates, outputIndex, target, inputs, g, l1);
        outputUpdateCounter.addTo(target, 1);
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
  }
}
