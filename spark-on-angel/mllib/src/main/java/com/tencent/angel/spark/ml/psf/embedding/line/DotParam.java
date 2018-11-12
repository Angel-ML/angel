package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.PooledByteBufAllocator;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class DotParam extends GetParam {

  int seed;
  int partitionId;
  int batchSize;
  byte[] edges;

  public DotParam(int matrixId,
                  int seed,
                  int partitionId,
                  int[] src,
                  int[] dst) {
    super(matrixId);
    this.seed = seed;
    this.partitionId = partitionId;
    this.batchSize = src.length;
    this.edges = new byte[8 * src.length];
    ByteBuffer wrapper = ByteBuffer.wrap(this.edges);
    for (int a = 0; a < batchSize; a++) {
      wrapper.putInt(src[a]);
      wrapper.putInt(dst[a]);
    }
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
          pkey,
          batchSize,
          edges));
    }
    return params;
  }
}
