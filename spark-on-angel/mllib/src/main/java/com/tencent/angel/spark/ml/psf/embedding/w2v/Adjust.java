package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;

public class Adjust extends UpdateFunc {

  public Adjust(AdjustParam param) {
    super(param);
  }

  public Adjust() { super(null); }

  public void partitionUpdate(PartitionUpdateParam partParam) {

    if (partParam instanceof AdjustPartitionParam) {

      AdjustPartitionParam param = (AdjustPartitionParam) partParam;

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
        int maxLength = ServerWrapper.getMaxLength();

        // compute number of nodes for one row
        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
        int numNodes = size / (partDim * order);

        int numRows = pkey.getEndRow() - pkey.getStartRow();
        float[][] layers = new float[numRows][];
        for (int row = 0; row < numRows; row ++)
          layers[row] = ((IntFloatDenseVectorStorage) psContext.getMatrixStorageManager()
                  .getRow(pkey, row).getSplit().getStorage())
                  .getValues();

        EmbeddingModel model;
        switch (param.model) {
          case 0:
            model = new SkipgramModel(partDim, negative, window, seed, maxIndex, numNodes, maxLength, layers);
            break;
          default:
            model = new CbowModel(partDim, negative, window, seed, maxIndex, numNodes, maxLength, layers);
        }


        int numInputs = ServerWrapper.getNumInputs(param.partitionId);
        int numOutputs = ServerWrapper.getNumOutputs(param.partitionId);

        model.adjust(sentences, param.buf, numInputs, numOutputs);
      } finally {
        param.clear();
      }
    }

  }

}
