package com.tencent.angel.spark.ml.psf.embedding;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;

public class Init extends UpdateFunc {

  public Init(InitParam param) {
    super(param);
  }

  public Init() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof InitPartitionParam) {

      InitPartitionParam param = (InitPartitionParam) partParam;
      ServerWrapper.initialize(param.numPartitions, param.maxIndex,
          param.maxLength, param.negative, param.order, param.partDim, param.window);
    }
  }
}
