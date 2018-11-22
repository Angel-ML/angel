package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DotParam extends GetParam {

  private byte[] dataBuf;
  private int bufLength;

  public DotParam(int matrixId,
                  int seed,
                  int partitionId,
                  int[] src,
                  int[] dst) {
    super(matrixId);
    this.bufLength = 12 + src.length * 8;
    this.dataBuf = new byte[bufLength];
    ByteBuffer wrapper = ByteBuffer.wrap(this.dataBuf);
    wrapper.putInt(seed);
    wrapper.putInt(partitionId);
    wrapper.putInt(src.length);
    for (int a = 0; a < src.length; a++) {
      wrapper.putInt(src[a]);
      wrapper.putInt(dst[a]);
    }
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> pkeys = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);
    List<PartitionGetParam> params = new ArrayList<>();
    for (PartitionKey pkey: pkeys) {
      params.add(new DotPartitionParam(matrixId, pkey, dataBuf, bufLength));
    }
    return params;
  }
}
