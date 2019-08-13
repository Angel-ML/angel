package com.tencent.angel.graph.client.initNeighbor5;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class InitNeighborPartParam extends PartitionUpdateParam {
  private Long2ObjectOpenHashMap<long[]> nodesToNeighbors;
  private long[] keys;
  private int[] index;
  private int[] indptr;
  private long[] neighbors;
  private int startIndex;
  private int endIndex;

  public InitNeighborPartParam(int matrixId, PartitionKey pkey,
                               long[] keys, int[] index, int[] indptr,
                               long[] neighbors, int startIndex,
                               int endIndex) {
    super(matrixId, pkey);
    this.keys = keys;
    this.index = index;
    this.indptr = indptr;
    this.neighbors = neighbors;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public InitNeighborPartParam() {
    this(0, null, null, null, null, null, 0, 0);
  }

  public Long2ObjectMap<long[]> getNodesToNeighbors() {
    return nodesToNeighbors;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(endIndex - startIndex);
//    System.out.println("serialize size=" + (endIndex - startIndex));
    for (int i = startIndex; i < endIndex; i++) {
      long key = keys[index[i]];
      int len = indptr[index[i] + 1] - indptr[index[i]];
      buf.writeLong(key);
      buf.writeInt(len);
      for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++)
        buf.writeLong(neighbors[j]);
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int size = buf.readInt();
    nodesToNeighbors = new Long2ObjectOpenHashMap<>(size);
    for (int i = 0; i < size; i++) {
      long node = buf.readLong();
      int len = buf.readInt();
      long[] neighbors = new long[len];
      for (int j = 0; j < len; j++)
        neighbors[j] = buf.readLong();
      nodesToNeighbors.put(node, neighbors);
    }
//    System.out.println("deserialize size=" + nodesToNeighbors.size());
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      len += 12;
      len += 8 * (indptr[index[i] + 1] - indptr[index[i]]);
    }
    return len;
  }
}
