package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;

public class Init extends UpdateFunc {

  public Init(InitParam param) {
    super(param);
  }

  public Init() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof InitPartitionParam) {

      InitPartitionParam param = (InitPartitionParam) partParam;
      ServerWrapper.initialize(param.numPartitions, param.maxIndex, param.maxLength);
    }
  }
}
