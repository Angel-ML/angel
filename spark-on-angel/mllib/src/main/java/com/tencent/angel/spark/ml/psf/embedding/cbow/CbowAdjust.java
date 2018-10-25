package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerPartition;
import com.tencent.angel.spark.ml.psf.embedding.NENegativeSample;
import com.tencent.angel.spark.ml.psf.embedding.ServerSentences;

import java.util.Arrays;
import java.util.Random;

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

        int[][] sentences = ServerSentences.getSentences(param.partitionId);

        // compute number of nodes for one row
        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
        int numNodes = size / (partDim * order);

        ServerPartition partition = psContext.getMatrixStorageManager().getPart(pkey);
        NENegativeSample sample = new NENegativeSample(-1, seed);
        Random rand = new Random(seed);

        int length = param.buf.readInt();

        // used to accumulate the context input vectors
        float[] neu1 = new float[partDim];
        float[] neu1e = new float[partDim];

        for (int s = 0; s < sentences.length; s++) {
          int[] sen = sentences[s];
          for (int position = 0; position < sen.length; position++) {
            int word = sen[position];
            // window size
            int b = rand.nextInt(window);
            Arrays.fill(neu1, 0);
            Arrays.fill(neu1e, 0);
            int cw = 0;

            for (int a = b; a < window * 2 + 1 - b; a++)
              if (a != window) {
                int c = position - window + a;
                if (c < 0) continue;
                if (c >= sen.length) continue;
                if (sen[c] == -1) continue;
                int row = sen[c] / numNodes;
                int col = (sen[c] % numNodes) * partDim * order;
                float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
                  .getSplit().getStorage()).getValues();
                for (c = 0; c < partDim; c++) neu1[c] += values[c + col];
                cw++;
              }

            if (cw > 0) {
              int target;
              for (int d = 0; d < negative + 1; d++) {
                if (d == 0) target = word;
                else target = sample.next(word);
                float g = param.buf.readFloat();
                length--;

                int row = target / numNodes;
                int col = (target % numNodes) * partDim * order + partDim;
                float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
                  .getSplit().getStorage()).getValues();
                // accumulate gradients for the input vectors
                for (int c = 0; c < partDim; c++) neu1e[c] += g * values[c + col];
                // update output vectors
                for (int c = 0; c < partDim; c++) values[c + col] += g * neu1[c];
              }

              // update input vectors
              for (int a = b; a < window * 2 + 1 - b; a++)
                if (a != window) {
                  int c = position - window + a;
                  if (c < 0) continue;
                  if (c >= sen.length) continue;
                  if (sen[c] == -1) continue;
                  int row = sen[c] / numNodes;
                  int col = (sen[c] % numNodes) * partDim * order;
                  float[] values = ((IntFloatDenseVectorStorage) partition.getRow(row)
                    .getSplit().getStorage()).getValues();
                  for (c = 0; c < partDim; c++) values[c + col] += neu1e[c];
                }
            }
          }
        }

        assert length == 0;
      } finally {
        param.clear();
      }
    }

  }
}
