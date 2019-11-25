package com.tencent.angel.graph.client.node2vec.getfuncs.getdegreebucket;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import io.netty.buffer.ByteBuf;

public class PullDegreeBucketPartitionParam extends PartitionGetParam {
  private int maxDegree;
  private int minDegree;
  private int numBuckets;

  public PullDegreeBucketPartitionParam(int matrixId, PartitionKey partKey, int maxDegree, int minDegree, int numBuckets) {
    super(matrixId, partKey);
    this.maxDegree = maxDegree;
    this.minDegree = minDegree;
    this.numBuckets = numBuckets;
  }

  public PullDegreeBucketPartitionParam(int maxDegree, int minDegree, int numBuckets) {
    this.maxDegree = maxDegree;
    this.minDegree = minDegree;
    this.numBuckets = numBuckets;
  }

  public PullDegreeBucketPartitionParam() {
    super();
  }

  public int getMaxDegree() {
    return maxDegree;
  }

  public void setMaxDegree(int maxDegree) {
    this.maxDegree = maxDegree;
  }

  public int getMinDegree() {
    return minDegree;
  }

  public void setMinDegree(int minDegree) {
    this.minDegree = minDegree;
  }

  public int getNumBuckets() {
    return numBuckets;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(maxDegree);
    buf.writeInt(minDegree);
    buf.writeInt(numBuckets);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    maxDegree = buf.readInt();
    minDegree = buf.readInt();
    numBuckets = buf.readInt();
  }

  @Override public int bufferLen() {
    return 12 + super.bufferLen();
  }

}
