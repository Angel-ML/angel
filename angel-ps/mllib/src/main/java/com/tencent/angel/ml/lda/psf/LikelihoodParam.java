package com.tencent.angel.ml.lda.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

public class LikelihoodParam extends GetParam {

  public static class LikelihoodPartParam extends PartitionGetParam {
    private float beta;
    public LikelihoodPartParam(int matrixId, PartitionKey pkey, float beta) {
      super(matrixId, pkey);
      this.beta = beta;
    }

    public LikelihoodPartParam() { super();}

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeFloat(beta);
    }

    @Override
    public void deserialize(ByteBuf buf) {
      super.deserialize(buf);
      beta = buf.readFloat();
    }

    @Override
    public int bufferLen() {
      return super.bufferLen() + 4;
    }

    public float getBeta() {
      return beta;
    }
  }

  private final float beta;

  public LikelihoodParam(int matrixId, float beta) {
    super(matrixId);
    this.beta = beta;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionGetParam> params = new ArrayList<>();
    List<PartitionKey> pkeys =
            PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);

    for (PartitionKey pkey : pkeys) {
      params.add(new LikelihoodPartParam(matrixId, pkey, beta));
    }

    return params;
  }
}
