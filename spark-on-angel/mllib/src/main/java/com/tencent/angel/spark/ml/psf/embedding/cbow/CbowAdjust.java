package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;

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
        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
        int numNodes = size / (partDim * order);

        int numRows = pkey.getEndRow() - pkey.getStartRow();
        float[][] layers = new float[numRows][];
        for (int row = 0; row < numRows; row ++)
          layers[row] = ((IntFloatDenseVectorStorage) psContext.getMatrixStorageManager()
                  .getRow(pkey, row).getSplit().getStorage())
                  .getValues();

        CbowModel cbow = new CbowModel(partDim, negative, window, seed, maxIndex, numNodes, 100, layers);

        int numInputs = ServerWrapper.getNumInputs(param.partitionId);
        int numOutputs = ServerWrapper.getNumOutputs(param.partitionId);

        cbow.adjust(sentences, param.buf, numInputs, numOutputs);
      } finally {
        param.clear();
      }
    }

  }

}
