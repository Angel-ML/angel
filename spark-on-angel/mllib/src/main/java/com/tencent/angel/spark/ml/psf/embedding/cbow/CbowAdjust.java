package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;

public class CbowAdjust extends UpdateFunc {

  public CbowAdjust(CbowAdjustParam param) {
    super(param);
  }

  public CbowAdjust() { super(null); }

  public void partitionUpdate(PartitionUpdateParam partParam) {

    if (partParam instanceof CbowAdjustPartitionParam) {

      CbowAdjustPartitionParam param = (CbowAdjustPartitionParam) partParam;

      try {
        // Some params
        PartitionKey pkey = param.getPartKey();
        int negative = param.negative;
        int partDim = param.partDim;
        int window = param.window;
        int seed = param.seed;
        int order = 2;

        int[][] sentences = param.sentences;
        int maxIndex = ServerWrapper.getMaxIndex();

        // compute number of nodes for one row
//        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
//        int numNodes = size / (partDim * order);

        ServerPartition partition = psContext.getMatrixStorageManager().getPart(pkey);
        Random windowSeed = new Random(seed);
        Random negativeSeed = new Random(seed + 1);

        int length = param.buf.readInt();

        // used to accumulate the context input vectors
        float[] neu1 = new float[partDim];
        float[] neu1e = new float[partDim];

        int numInputs = ServerWrapper.getNumInputs(param.partitionId, param.threadId);
        int numOutputs = ServerWrapper.getNumOutputs(param.partitionId, param.threadId);

        float[] inputs = new float[numInputs * partDim];
        float[] outputs = new float[numOutputs * partDim];

        Int2IntOpenHashMap inputIndex = new Int2IntOpenHashMap();
        Int2IntOpenHashMap outputIndex = new Int2IntOpenHashMap();

        Int2IntOpenHashMap inputUpdateCounter = new Int2IntOpenHashMap();
        Int2IntOpenHashMap outputUpdateCounter = new Int2IntOpenHashMap();

        int[] windows = new int[window * 2];

        // We assume that there are only one row in the partition
        float[] values = ((IntFloatDenseVectorStorage) partition.getRow(0).getSplit().getStorage()).getValues();
        // We assume that the max_length of one doc is 200.
        float[] sentence_vectors = new float[partDim * 200];

        for (int s = 0; s < sentences.length; s++) {
          int[] sen = sentences[s];

          // locates the input vector into local arrays to prevent randomly access for
          // the large server row.
          for (int a = 0; a < sen.length; a ++) {
            int node = sen[a];
            int offset = node * partDim * order;
            int start  = a * partDim;
            for (int c = 0; c < partDim; c ++)
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
                int start = c * partDim;
                for (c = 0; c < partDim; c++) neu1[c] += sentence_vectors[c + start];
                cw++;
              }


            if (cw > 0) {
              for (int c = 0; c < partDim; c ++) neu1[c] /= cw;
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

                float g = param.buf.readFloat();
                length--;
                // how to prevent the randomly access to the output vectors??
                int col = target * partDim * order + partDim;
                // accumulate gradients for the input vectors
                for (int c = 0; c < partDim; c++) neu1e[c] += g * values[c + col];

                // update output vectors
                merge(outputs, outputIndex, target, neu1, g);
                outputUpdateCounter.addTo(target, 1);
              }

              for (int a = 0; a < cw; a ++) {
                int input = windows[a];
                merge(inputs, inputIndex, input, neu1e, 1);
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
          int offset = entry.getIntValue() * partDim;
          int divider = inputUpdateCounter.get(node);
          int col = node * partDim * order;
          for (int a = 0; a < partDim; a++) values[a + col] += inputs[offset + a] / divider;
        }

        // update output
        it = outputIndex.int2IntEntrySet().fastIterator();
        while (it.hasNext()) {
          Int2IntMap.Entry entry = it.next();
          int node = entry.getIntKey();
          int offset = entry.getIntValue() * partDim;
          int col = node * partDim * order + partDim;
          int divider = outputUpdateCounter.get(node);
          for (int a = 0; a < partDim; a++) values[a + col] += outputs[offset + a] / divider;
        }

        assert length == 0;
      } finally {
        param.clear();
      }
    }

  }

  private void merge(float[] inputs, Int2IntOpenHashMap inputIndex, int node, float[] update, float g) {
    int start = inputIndex.get(node);
    if (!inputIndex.containsKey(node)) {
      start = inputIndex.size();
      inputIndex.put(node, start);
    }

    int offset = start * update.length;
    for (int c = 0; c < update.length; c ++) inputs[offset + c] += g * update[c];
  }



}
