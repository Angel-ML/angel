package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DotParam extends GetParam {

  int seed;
  int partitionId;
  int model;
  int[][] sentences;

  public DotParam(int matrixId,
                  int seed,
                  int partitionId,
                  int model,
                  int[][] sentences) {
    super(matrixId);
    this.seed = seed;
    this.partitionId = partitionId;
    this.model = model;
    this.sentences = sentences;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionGetParam> params = new ArrayList<>();
    Iterator<PartitionKey> iterator = pkeys.iterator();
    while (iterator.hasNext()) {
      PartitionKey pkey = iterator.next();
      params.add(new DotPartitionParam(matrixId,
              seed,
              partitionId,
              model,
              pkey,
              sentences));
    }
    return params;
  }
}
