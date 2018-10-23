package com.tencent.angel.spark.ml.psf.embedding.CBOW;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class CBOWDot extends GetFunc {

  class CBOWDotPartitionParam extends PartitionGetParam {

    private int seed;
    private int negative;
    private int window;
    private int partDim;
    private int[][] sentences;

    public CBOWDotPartitionParam(int matrixId,
                                 int seed,
                                 int negative,
                                 int window,
                                 int partDim,
                                 int[][] sentences,
                                 PartitionKey pkey) {
      super(matrixId, pkey);
      this.seed = seed;
      this.negative = negative;
      this.window = window;
      this.partDim = partDim;
      this.sentences = sentences;
    }

    @Override
    public void serialize(ByteBuf buf) {
      super.serialize(buf);
      buf.writeInt(seed);
      buf.writeInt(negative);
      buf.writeInt(window);
      buf.writeInt(partDim);
      buf.writeInt(sentences.length);
      for (int a = 0; a < sentences.length; a ++) {
        buf.writeInt(sentences[a].length);
        for (int b = 0; b < sentences[a].length; b ++)
          buf.writeInt(sentences[a][b]);
      }
    }

    @Override
    public void deserialize(ByteBuf buf) {

    }
  }

  class CBOWDotParam extends GetParam {
    @Override
    public List<PartitionGetParam> split() {
      return super.split();
    }
  }

  public CBOWDot(GetParam param) {
    super(param);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return null;
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    return null;
  }
}
