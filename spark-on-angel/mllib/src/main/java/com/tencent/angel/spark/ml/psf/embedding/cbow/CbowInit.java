package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.spark.ml.psf.embedding.ServerWrapper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class CbowInit extends UpdateFunc {

  private static final Log LOG = LogFactory.getLog(CbowInit.class);

  public CbowInit(CbowInitParam param) {
    super(param);
  }

  public CbowInit() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof CbowInitPartitionParam) {
      CbowInitPartitionParam param = (CbowInitPartitionParam) partParam;

      ServerWrapper.initialize(param.numPartitions, param.concurrentLevel);
      ServerWrapper.setMaxIndex(param.maxIndex);

    }
  }
}
