package com.tencent.angel.spark.ml.psf.embedding.w2v;

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

  public InitParam(int matrixId,
                   int numPartitions,
                   int maxIndex,
                   int maxLength) {
    super(matrixId);

    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.maxLength = maxLength;
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
                maxLength));
      }
    }
    return params;
  }
}
