package com.tencent.angel.spark.ml.psf.embedding.line;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class AdjustParam extends UpdateParam {

  private byte[] dataBuf;
  private int bufLength;

  public AdjustParam(int matrixId,
                     int seed,
                     int negative,
                     int partitionId,
                     float[] gradient,
                     int[] src,
                     int[] dst) {
    super(matrixId);
    this.bufLength = 12 + src.length * 8 + gradient.length * 4;
    this.dataBuf = new byte[bufLength];
    ByteBuffer wrapper = ByteBuffer.wrap(this.dataBuf);
    wrapper.putInt(seed);
    wrapper.putInt(partitionId);
    wrapper.putInt(src.length);
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
      AdjustPartitionParam partParam = new AdjustPartitionParam(matrixId, pkey, dataBuf, bufLength);
      params.add(partParam);
    }
    return params;
  }
}

