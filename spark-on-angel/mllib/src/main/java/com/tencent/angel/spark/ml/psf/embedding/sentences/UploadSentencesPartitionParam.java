package com.tencent.angel.spark.ml.psf.embedding.sentences;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class UploadSentencesPartitionParam extends PartitionUpdateParam {

  int partitionId;
  boolean initialize;
  int numPartitions;
  int maxIndex;
  int[][] sentences;

  public UploadSentencesPartitionParam(int matrixId,
                                       PartitionKey partKey,
                                       int partitionId,
                                       int numPartitions,
                                       int maxIndex,
                                       boolean initialize,
                                       int[][] sentences) {
    super(matrixId, partKey);
    this.partitionId = partitionId;
    this.numPartitions = numPartitions;
    this.maxIndex = maxIndex;
    this.initialize = initialize;
    this.sentences = sentences;
  }

  public UploadSentencesPartitionParam() {}

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(partitionId);
    buf.writeInt(numPartitions);
    buf.writeInt(maxIndex);
    buf.writeBoolean(initialize);
    buf.writeInt(sentences.length);
    for (int a = 0; a < sentences.length; a++) {
      buf.writeInt(sentences[a].length);
      for (int b = 0; b < sentences[a].length; b++) buf.writeInt(sentences[a][b]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    partitionId = buf.readInt();
    numPartitions = buf.readInt();
    maxIndex = buf.readInt();
    initialize  = buf.readBoolean();
    int length = buf.readInt();
    sentences = new int[length][];
    for (int a = 0; a < length; a++) {
      sentences[a] = new int[buf.readInt()];
      for (int b = 0; b < sentences[a].length; b ++)
        sentences[a][b] = buf.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return super.bufferLen();
  }
}
