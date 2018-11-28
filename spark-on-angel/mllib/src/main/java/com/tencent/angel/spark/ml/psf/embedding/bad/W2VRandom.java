package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;

import java.util.Random;

public class W2VRandom extends UpdateFunc {


  public W2VRandom(int matrixId, int dimension) {
    this(new W2VRandomParam(matrixId, dimension));
  }

  public W2VRandom(UpdateParam param) {
    super(param);
  }

  public W2VRandom() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof W2VRandomPartitionParam) {
      W2VRandomPartitionParam param = (W2VRandomPartitionParam) partParam;
      int dimension = param.dimension;
      ServerPartition partition = psContext.getMatrixStorageManager()
        .getPart(param.getPartKey());
      update(partition, param.getPartKey(), dimension);
    }
  }

  private void update(ServerPartition partition,
                      PartitionKey pkey,
                      int dimension) {
    int startRow = pkey.getStartRow();
    int endRow   = pkey.getEndRow();

    Random random = new Random(System.currentTimeMillis());
    for (int r = startRow; r < endRow; r ++) {
      float[] values = ((IntFloatDenseVectorStorage) partition.getRow(r)
        .getSplit().getStorage())
        .getValues();

      assert values.length % (dimension * 2) == 0;
      int numRows = values.length / dimension / 2;
      for (int a = 0; a < numRows; a ++) {
        int offset = a * dimension * 2;
        for (int b = 0; b < dimension; b ++)
//          values[b + offset] = (random.nextFloat() - 0.5f) / dimension;
          values[b + offset] = 0.01f;
      }
    }
  }
}
