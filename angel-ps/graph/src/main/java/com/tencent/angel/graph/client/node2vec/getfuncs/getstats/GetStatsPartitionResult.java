package com.tencent.angel.graph.client.node2vec.getfuncs.getstats;

import com.tencent.angel.graph.client.node2vec.utils.SerDe;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;

public class GetStatsPartitionResult extends PartitionGetResult {

  private long numNode = 0;
  private long numEdge = 0;
  private Int2IntOpenHashMap buckets;

  public GetStatsPartitionResult(long numNode, long numEdge, Int2IntOpenHashMap buckets) {
    this.numNode = numNode;
    this.numEdge = numEdge;
    this.buckets = buckets;
  }

  public GetStatsPartitionResult() {
  }

  public long getNumNode() {
    return numNode;
  }

  public void setNumNode(long numNode) {
    this.numNode = numNode;
  }

  public long getNumEdge() {
    return numEdge;
  }

  public void setNumEdge(long numEdge) {
    this.numEdge = numEdge;
  }

  public Int2IntOpenHashMap getBuckets() {
    return buckets;
  }

  public void setBuckets(Int2IntOpenHashMap buckets) {
    this.buckets = buckets;
  }

  @Override
  public void serialize(ByteBuf output) {
    output.writeLong(numNode);
    output.writeLong(numEdge);
    SerDe.serInt2IntMap(buckets, output);
  }

  @Override
  public void deserialize(ByteBuf input) {
    numNode = input.readLong();
    numEdge = input.readLong();
    buckets = SerDe.deserInt2IntMap(input);
  }

  @Override
  public int bufferLen() {
    return 20 + buckets.size() * 8;
  }
}
