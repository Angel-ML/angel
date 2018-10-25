package com.tencent.angel.spark.ml.psf.embedding.sentences;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;

import java.util.ArrayList;
import java.util.List;

public class UploadSentencesParam extends UpdateParam {
  private int partitionId;
  private int numPartitions;
  private boolean initialize;
  private int[][] sentences;
  public UploadSentencesParam(int matrixId,
                              int partitionId,
                              int numPartitions,
                              boolean initialize,
                              int[][] sentences) {
    super(matrixId);
    this.partitionId = partitionId;
    this.numPartitions = numPartitions;
    this.initialize  = initialize;
    this.sentences   = sentences;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager()
      .getPartitions(matrixId);

    IntOpenHashSet serverIds = new IntOpenHashSet();
    List<PartitionUpdateParam> params = new ArrayList<>();
    for (PartitionKey pkey: pkeys) {
      int serverId = PSAgentContext.get().getMatrixMetaManager()
        .getMasterPS(pkey).getIndex();
      if (!serverIds.contains(serverId)) {
        serverIds.add(serverId);
        params.add(new UploadSentencesPartitionParam(matrixId,
          pkey,
          partitionId,
          numPartitions,
          initialize,
          sentences));
      }
    }
    return params;
  }
}
