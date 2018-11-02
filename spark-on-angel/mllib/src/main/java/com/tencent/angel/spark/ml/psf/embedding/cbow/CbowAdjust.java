package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.*;

public class CbowAdjust extends UpdateFunc {

  public CbowAdjust(CbowAdjustParam param) {
    super(param);
  }

  public CbowAdjust() { super(null); }

  @Override
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

        int[][] sentences = ServerWrapper.getSentencesWithPartitionId(param.partitionId);
        int maxIndex = ServerWrapper.getMaxIndex();

        // compute number of nodes for one row
        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
        int numNodes = size / (partDim * order);

        ServerPartition partition = psContext.getMatrixStorageManager().getPart(pkey);
        Random windowSeed = new Random(seed);
        Random negativeSeed = new Random(seed + 1);

        int length = param.buf.readInt();

        // used to accumulate the context input vectors
        float[] neu1 = new float[partDim];
        float[] neu1e = new float[partDim];

        int contextIdx = 0;
        float[] contextCache = ServerWrapper.getContext(param.partitionId);

        Map<Integer, float[]> outputUpdates = new HashMap<>();
        Map<Integer, float[]> inputUpdates  = new HashMap<>();
        Int2IntOpenHashMap inputUpdateCounter = new Int2IntOpenHashMap();
        Int2IntOpenHashMap outputUpdateCounter = new Int2IntOpenHashMap();

        for (int s = 0; s < sentences.length; s++) {
          int[] sen = sentences[s];
          for (int position = 0; position < sen.length; position++) {
            int word = sen[position];
            // window size
            int b = windowSeed.nextInt(window);
            Arrays.fill(neu1, 0);
            Arrays.fill(neu1e, 0);
            int cw = 0;

//            for (int a = b; a < window * 2 + 1 - b; a++)
//              if (a != window) {
//                int c = position - window + a;
//                if (c < 0) continue;
//                if (c >= sen.length) continue;
//                if (sen[c] == -1) continue;
////                int row = sen[c] / numNodes;
////                int col = (sen[c] % numNodes) * partDim * order;
////                float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
////                  .getSplit().getStorage()).getValues();
////                for (c = 0; c < partDim; c++) neu1[c] += values[c + col];
//                cw++;
//              }



//            if (cw > 0) {
              for (int c = 0; c < partDim; c ++) neu1[c] = contextCache[contextIdx++];
//              for (int c = 0; c < partDim; c ++) neu1[c] /= cw;
              int target;
              for (int d = 0; d < negative + 1; d++) {
                if (d == 0) target = word;
                else
                  while (true) {
                    target = negativeSeed.nextInt(maxIndex);
                    if (target == word) continue;
                    else break;
                  }

                float g = param.buf.readFloat();
                length--;

                int row = target / numNodes;
                int col = (target % numNodes) * partDim * order + partDim;
                float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
                  .getSplit().getStorage()).getValues();
                // accumulate gradients for the input vectors
                for (int c = 0; c < partDim; c++) neu1e[c] += g * values[c + col];
                // update output vectors
                mergeUpdates(outputUpdates, target, neu1, g);
                outputUpdateCounter.addTo(target, 1);
              }

              // update input vectors
              for (int a = b; a < window * 2 + 1 - b; a++)
                if (a != window) {
                  int c = position - window + a;
                  if (c < 0) continue;
                  if (c >= sen.length) continue;
                  if (sen[c] == -1) continue;
                  mergeUpdates(inputUpdates, sen[c], neu1e, 1);
                  inputUpdateCounter.addTo(sen[c], 1);
                }
            }
          }
//        }

        // conduct update
        Iterator<Map.Entry<Integer, float[]>> iterator = inputUpdates.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<Integer, float[]> entry = iterator.next();
          int node = entry.getKey();
          float[] update = entry.getValue();
          int row = node / numNodes;
          int col = (node % numNodes) * partDim * order;
          float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
                  .getSplit().getStorage()).getValues();
          int divider = inputUpdateCounter.get(node);
          for (int a = 0; a < partDim; a++) values[a + col] += update[a] / divider;

        }

        iterator = outputUpdates.entrySet().iterator();
        while (iterator.hasNext()) {
          Map.Entry<Integer, float[]> entry = iterator.next();
          int node = entry.getKey();
          float[] update = entry.getValue();
          int row = node / numNodes;
          int col = (node % numNodes) * partDim * order + partDim;
          float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
                  .getSplit().getStorage()).getValues();
          int divider = outputUpdateCounter.get(node);
          for (int a = 0; a < partDim; a++) values[a + col] += update[a] / divider;
        }

        assert length == 0;
      } finally {
        param.clear();
      }
    }
  }

  private void mergeUpdates(Map<Integer, float[]> bufferedUpdate, int node, float[] update, float g) {
    if (!bufferedUpdate.containsKey(node))
      bufferedUpdate.put(node, new float[update.length]);

    float[] hold = bufferedUpdate.get(node);
    for (int a = 0; a < hold.length; a++) hold[a] += update[a] * g;
  }
}
