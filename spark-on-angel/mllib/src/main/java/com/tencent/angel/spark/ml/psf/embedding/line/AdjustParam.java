package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.PooledByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AdjustParam extends UpdateParam {

  private int seed;
  private int partitionId;
  private byte[] edgesAndGrads;
  private int batchSize;
  private int negative;

  public AdjustParam(int matrixId,
                     int seed,
                     int negative,
                     int partitionId,
                     float[] gradient,
                     int[] src,
                     int[] dst) {
    super(matrixId);
    this.seed = seed;
    this.partitionId = partitionId;
    this.batchSize = src.length;
    this.negative = negative;
    this.edgesAndGrads = new byte[batchSize * (4 * negative + 12)];
    ByteBuffer wrapper = ByteBuffer.wrap(this.edgesAndGrads);
    int inc = 0;
    for (int a = 0; a < src.length; a++) {
      wrapper.putInt(src[a]);
      wrapper.putInt(dst[a]);
      for (int b = 0; b < negative + 1; b++) {
        wrapper.putFloat(gradient[inc++]);
      }
    }
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
          partitionId,
          batchSize,
          edgesAndGrads));
    }
    return params;
  }
}

