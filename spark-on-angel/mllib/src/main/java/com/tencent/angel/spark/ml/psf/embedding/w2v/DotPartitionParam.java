package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class DotPartitionParam extends PartitionGetParam {

  int seed;
  int negative;
  int window;
  int partDim;
  int partitionId;
  int model;
  int[][] sentences;

  public DotPartitionParam(int matrixId,
                           int seed,
                           int negative,
                           int window,
                           int partDim,
                           int partitionId,
                           int model,
                           PartitionKey pkey,
                           int[][] sentences) {
    super(matrixId, pkey);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
    this.model = model;
    this.sentences = sentences;
  }

  public DotPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(seed);
    buf.writeInt(negative);
    buf.writeInt(window);
    buf.writeInt(partDim);
    buf.writeInt(partitionId);
    buf.writeInt(model);

    buf.writeInt(sentences.length);
    for (int a = 0; a < sentences.length; a ++) {
      buf.writeInt(sentences[a].length);
      for (int b = 0; b < sentences[a].length; b ++)
        buf.writeInt(sentences[a][b]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    this.seed = buf.readInt();
    this.negative = buf.readInt();
    this.window = buf.readInt();
    this.partDim = buf.readInt();
    this.partitionId = buf.readInt();
    this.model = buf.readInt();

    int length = buf.readInt();
    sentences = new int[length][];
    for (int a = 0; a < length; a++) {
      sentences[a] = new int[buf.readInt()];
      for (int b = 0; b < sentences[a].length; b ++)
        sentences[a][b] = buf.readInt();
    }
  }
}
