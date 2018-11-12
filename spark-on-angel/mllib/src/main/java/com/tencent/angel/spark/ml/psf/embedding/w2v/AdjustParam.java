package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

public class AdjustParam extends UpdateParam {

  private int seed;
  private int negative;
  private int window;
  private int partDim;
  private int partitionId;
  private int model;
  private float[] gradient;
  private int[][] sentences;

  public AdjustParam(int matrixId,
                     int seed,
                     int negative,
                     int window,
                     int partDim,
                     int partitionId,
                     int model,
                     float[] gradient,
                     int[][] sentences) {
    super(matrixId);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
    this.model = model;
    this.gradient = gradient;
    this.sentences = sentences;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager()
            .getPartitions(matrixId);
    List<PartitionUpdateParam> params = new ArrayList<>();
    for (PartitionKey pkey : pkeys) {
      params.add(new AdjustPartitionParam(matrixId,
              pkey,
              seed,
              negative,
              window,
              partDim,
              partitionId,
              model,
              gradient,
              sentences));
    }
    return params;
  }
}

