package com.tencent.angel.spark.ml.psf.embedding.w2v;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class AdjustPartitionParam extends PartitionUpdateParam {

  int seed;
  int negative;
  int window;
  int partDim;
  int partitionId;
  int model;
  private float[] gradient;
  int[][] sentences;
  ByteBuf buf;

  public AdjustPartitionParam(int matrixId,
                              PartitionKey partKey,
                              int seed,
                              int negative,
                              int window,
                              int partDim,
                              int partitionId,
                              int model,
                              float[] gradient,
                              int[][] sentences) {
    super(matrixId, partKey);
    this.seed = seed;
    this.negative = negative;
    this.window = window;
    this.partDim = partDim;
    this.partitionId = partitionId;
    this.model = model;
    this.gradient = gradient;
    this.sentences = sentences;
  }

  public AdjustPartitionParam() {}

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

    buf.writeInt(gradient.length);
    for (int a = 0; a < gradient.length; a++) buf.writeFloat(gradient[a]);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    seed = buf.readInt();
    negative = buf.readInt();
    window = buf.readInt();
    partDim = buf.readInt();
    partitionId = buf.readInt();
    model = buf.readInt();

    int length = buf.readInt();
    sentences = new int[length][];
    for (int a = 0; a < length; a++) {
      sentences[a] = new int[buf.readInt()];
      for (int b = 0; b < sentences[a].length; b ++)
        sentences[a][b] = buf.readInt();
    }

    this.buf = buf;
    buf.retain();
  }

  public void clear() {
    buf.release();
  }

  @Override
  public int bufferLen() {
    return super.bufferLen() + 24 + 4 + gradient.length * 4;
  }
}
