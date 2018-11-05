package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CbowDotParam extends GetParam {

  int seed;
  int negative;
  int window;
  int partDim;
  int partitionId;
  int threadId;
  int[][] sentences;

  public CbowDotParam(int matrixId,
                      int seed,
                      int negative,
                      int window,
                      int partDim,
                      int partitionId,
                      int threadId,
                      int[][] sentences) {
    super(matrixId);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
    this.threadId = threadId;
    this.sentences = sentences;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionGetParam> params = new ArrayList<>();
    Iterator<PartitionKey> iterator = pkeys.iterator();
    while (iterator.hasNext()) {
      PartitionKey pkey = iterator.next();
      params.add(new CbowDotPartitionParam(matrixId,
              seed,
              negative,
              window,
              partDim,
              partitionId,
              threadId,
              pkey,
              sentences));
    }
    return params;
  }
}