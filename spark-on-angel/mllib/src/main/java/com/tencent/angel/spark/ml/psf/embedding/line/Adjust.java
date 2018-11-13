package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import io.netty.buffer.ByteBuf;

public class Adjust extends UpdateFunc {

  public Adjust(AdjustParam param) {
    super(param);
  }

  public Adjust() {
    super(null);
  }

  public void partitionUpdate(PartitionUpdateParam partParam) {

    if (partParam instanceof AdjustPartitionParam) {

      AdjustPartitionParam param = (AdjustPartitionParam) partParam;

      try {
        // Some params
        PartitionKey pkey = param.getPartKey();
        int seed = param.seed;
        ByteBuf dataBuf = param.edgesAndGradsBuf;

        int negative = ServerWrapper.getNegative();
        int maxIndex = ServerWrapper.getMaxIndex();
        int order = ServerWrapper.getOrder();
        int partDim = ServerWrapper.getPartDim();

        // compute number of nodes for one row
        int size = (int) (pkey.getEndCol() - pkey.getStartCol());
        int numNodes = size / (partDim * order);

        int numRows = pkey.getEndRow() - pkey.getStartRow();
        float[][] layers = new float[numRows][];
        for (int row = 0; row < numRows; row++)
          layers[row] = ((IntFloatDenseVectorStorage) psContext.getMatrixStorageManager()
              .getRow(pkey, row).getSplit().getStorage())
              .getValues();

        EmbeddingModel model;
        switch (order) {
          case 1:
            model = new LINEFirstOrderModel(partDim, negative, seed, maxIndex, numNodes, layers);
            break;
          default:
            model = new LINESecondOrderModel(partDim, negative, seed, maxIndex, numNodes, layers);
        }


        int numInputs = ServerWrapper.getNumInputs(param.partitionId);
        int numOutputs = ServerWrapper.getNumOutputs(param.partitionId);

        model.adjust(dataBuf, numInputs, numOutputs);
      } finally {
        param.clear();
      }
    }
  }
}
