package com.tencent.angel.graph.kclique;


import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

public class KCliquePartUpdateParam extends PartitionUpdateParam {

  /**
   * Node id to neighbors map
   */
  private Long2ObjectMap<long[]> nodeIdToNeighborIndices;

  private long[] nodeIds;
  private transient int startIndex;
  private transient int endIndex;
  private transient int writeRowId;

  public KCliquePartUpdateParam(int matrixId, PartitionKey partKey,
                                Long2ObjectMap<long[]> nodeIdToNeighborIndices, long[] nodeIds, int startIndex,
                                int endIndex, int rowId) {
    super(matrixId, partKey);
    this.nodeIdToNeighborIndices = nodeIdToNeighborIndices;
    this.nodeIds = nodeIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
    this.writeRowId = rowId;
  }

  public KCliquePartUpdateParam() {
    this(0, null, null, null, 0, 0, 0);
  }

  public Long2ObjectMap<long[]> getNodeIdToNeighborIndices() {
    return nodeIdToNeighborIndices;
  }

  public int getWriteRowId() {
    return writeRowId;
  }

  private void clear() {
    nodeIdToNeighborIndices = null;
    nodeIds = null;
    startIndex = -1;
    endIndex = -1;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    long nodeId;
    long[] neighbors;
    int writeIndex = buf.writerIndex();
    int writeNum = 0;
    buf.writeInt(0);
    buf.writeInt(writeRowId);
    for (int i = startIndex; i < endIndex; i++) {
      nodeId = nodeIds[i];
      neighbors = nodeIdToNeighborIndices.get(nodeId);
      if (neighbors == null || neighbors.length == 0) {
        continue;
      }
      buf.writeLong(nodeId);
      buf.writeInt(neighbors.length);
      for (long neighbor : neighbors) {
        buf.writeLong(neighbor);
      }
      writeNum++;
    }
    buf.setInt(writeIndex, writeNum);

  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    int len = buf.readInt();
    writeRowId = buf.readInt();
    nodeIdToNeighborIndices = new Long2ObjectArrayMap<>(len);

    for (int i = 0; i < len; i++) {
      long nodeId = buf.readLong();
      int neighborNum = buf.readInt();
      long[] neighbor = new long[neighborNum];
      for (int j = 0; j < neighborNum; j++) {
        neighbor[j] = buf.readLong();
      }
      nodeIdToNeighborIndices.put(nodeId, neighbor);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    len += 4;
    long nodeId;
    long[] neighbors;
    for (int i = startIndex; i < endIndex; i++) {
      nodeId = nodeIds[i];
      neighbors = nodeIdToNeighborIndices.get(nodeId);
      if (neighbors == null || neighbors.length == 0) {
        continue;
      }
      len += 12;
      len += 8 * nodeIdToNeighborIndices.get(nodeIds[i]).length;

    }
    return len;
  }
}
