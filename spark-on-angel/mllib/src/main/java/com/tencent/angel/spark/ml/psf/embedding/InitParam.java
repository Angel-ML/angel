package com.tencent.angel.spark.ml.psf.embedding;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.ArrayList;
import java.util.List;

public class InitParam extends UpdateParam {


  private int numPartitions;
  private int maxIndex;
  private int maxLength;
  private int negative;
  private int order;
  private int partDim;
  private int window;

  public InitParam(int matrixId,
                   int numPartitions,
                   int maxIndex,
                   int maxLength,
                   int negative,
                   int order,
                   int partDim,
                   int window) {
    super(matrixId);

    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.maxLength = maxLength;
    this.negative = negative;
    this.order = order;
    this.partDim = partDim;
    this.window = window;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager()
        .getPartitions(matrixId);

    IntOpenHashSet serverIds = new IntOpenHashSet();
    List<PartitionUpdateParam> params = new ArrayList<>();
    for (PartitionKey pkey : pkeys) {
      int serverId = PSAgentContext.get().getMatrixMetaManager()
          .getMasterPS(pkey).getIndex();
      if (!serverIds.contains(serverId)) {
        serverIds.add(serverId);
        params.add(new InitPartitionParam(matrixId,
            pkey,
            numPartitions,
            maxIndex,
            maxLength,
            negative,
            order,
            partDim,
            window));
      }
    }
    return params;
  }
}
