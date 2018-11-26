package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.storage.IntFloatDenseVectorStorage;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerPartition;

public class W2VPush extends UpdateFunc {

  public W2VPush(UpdateParam param) {
    super(param);
  }

  public W2VPush() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof W2VPushPartitionParam) {
      W2VPushPartitionParam param = (W2VPushPartitionParam) partParam;
      try {
        update(psContext.getMatrixStorageManager().getPart(param.getPartKey()),
                param);
      } finally {
        param.clear();
      }
    }
  }

  private void update(ServerPartition partition,
                      W2VPushPartitionParam param) {
    PartitionKey pkey = param.getPartKey();
    int totalRows = pkey.getEndRow() - pkey.getStartRow();
    int startRow  = pkey.getStartRow();
    float[][] rows = new float[totalRows][];
    int numNodePerRow = param.numNodePerRow;
    int startNode = startRow * numNodePerRow;
    int dimension = param.dimension;

    for (int row = startRow; row < startRow + totalRows; row ++)
      rows[row - startRow] = ((IntFloatDenseVectorStorage) partition
              .getRow(row).getSplit().getStorage())
              .getValues();

    for (int i = 0; i < param.length; i++) {
      int node = param.buf.readInt();
      int rowId  = (node - startNode) / numNodePerRow;
      int offset  = (node % numNodePerRow) * dimension * 2;
      float[] values = rows[rowId];
      for (int d = 0; d < dimension * 2; d ++) {
        values[offset + d] += param.buf.readFloat();
      }
    }
  }
}
