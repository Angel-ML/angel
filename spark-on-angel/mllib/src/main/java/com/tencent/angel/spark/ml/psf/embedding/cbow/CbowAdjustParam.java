package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class CbowAdjustParam extends UpdateParam {

  private int seed;
  private int negative;
  private int window;
  private int partDim;
  private int partitionId;
  private float[] gradient;

  public CbowAdjustParam(int matrixId,
                         int seed,
                         int negative,
                         int window,
                         int partDim,
                         int partitionId,
                         float[] gradient) {
    super(matrixId);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
    this.gradient = gradient;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(matrixId);
    List<PartitionUpdateParam> params = new ArrayList<>();
    for (PartitionKey pkey : pkeys) {
      params.add(new CbowAdjustPartitionParam(matrixId,
        pkey,
        seed,
        negative,
        window,
        partDim,
        partitionId,
        gradient));
    }

    return params;
  }
}

