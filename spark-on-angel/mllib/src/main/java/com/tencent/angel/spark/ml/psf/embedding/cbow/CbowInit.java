package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CbowInit extends UpdateFunc {

  public CbowInit(CbowInitParam param) {
    super(param);
  }

  public CbowInit() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof CbowInitPartitionParam) {

      CbowInitPartitionParam param = (CbowInitPartitionParam) partParam;
      System.out.println("init maxIndex=" + param.maxIndex);
      ServerWrapper.initialize(param.numPartitions, param.maxIndex, param.maxLength);
    }
  }
}
