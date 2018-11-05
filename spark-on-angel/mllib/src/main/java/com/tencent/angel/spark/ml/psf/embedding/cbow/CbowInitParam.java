package com.tencent.angel.spark.ml.psf.embedding.cbow;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.ArrayList;
import java.util.List;

public class CbowInitParam extends UpdateParam {

  private int partitionId;
  private int numPartitions;
  private int maxIndex;
  private int concurrentLevel;

  public CbowInitParam(int matrixId,
                       int partitionId,
                       int numPartitions,
                       int maxIndex,
                       int concurrentLevel) {
    super(matrixId);
    this.partitionId = partitionId;
    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.concurrentLevel = concurrentLevel;
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
        params.add(new CbowInitPartitionParam(matrixId,
                pkey,
                partitionId,
                numPartitions,
                maxIndex,
                concurrentLevel));
      }
    }
    return params;
  }
}
