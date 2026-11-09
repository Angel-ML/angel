package com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.initNeighborsRandomTable;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.sampleRandomNeighborTable.NeighborsRandomTableElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

/**
 * part node's data in ps
 */
public class PartInitNeighborRandomTableParam extends PartitionUpdateParam {

  private Long2ObjectMap<NeighborsRandomTableElement> nodeId2Neighbors;

  private long[] nodeIds;
  private transient int startIndex;
  private transient int endIndex;

  public PartInitNeighborRandomTableParam(int matrixId, PartitionKey partitionKey,
      Long2ObjectMap<NeighborsRandomTableElement> nodeId2Neighbors, long[] nodeIds, int startIndex,
      int endIndex) {

    super(matrixId, partitionKey);
    this.nodeId2Neighbors = nodeId2Neighbors;
    this.nodeIds = nodeIds;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public PartInitNeighborRandomTableParam() {
    this(0, null, null, null, 0, 0);
  }

  private void clear() {
    nodeId2Neighbors = null;
    nodeIds = null;
    startIndex = -1;
    endIndex = -1;
  }

  public Long2ObjectMap<NeighborsRandomTableElement> getNodeId2Neighbors() {
    return nodeId2Neighbors;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);

    long nodeId;
    NeighborsRandomTableElement neighbors;
    int writeIndex = buf.writerIndex();
    int numNodesWriten = 0;

    buf.writeInt(0);

    for (int i = startIndex; i < endIndex; i++) {
      // get nodeId
      nodeId = nodeIds[i];
      //get  data the nodeId mapped neighbors
      neighbors = nodeId2Neighbors.get(nodeId);
      if (neighbors == null) {
        continue;
      }
      buf.writeLong(nodeId);
      buf.writeInt(neighbors.getNodesNum());

      // write out edges
      long[] nbrs = neighbors.getNeighborIds();
      for (int j = 0; j < nbrs.length; j++) {
        buf.writeLong(nbrs[j]);
      }

      // write out weights
      float[] weights = neighbors.getWeights();
      assert (weights.length == nbrs.length);
      for (int j = 0; j < weights.length; j++) {
        buf.writeFloat(weights[j]);
      }

      numNodesWriten++;
    }

    buf.setInt(writeIndex, numNodesWriten);
    clear();
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);

    int len = buf.readInt();
    nodeId2Neighbors = new Long2ObjectArrayMap<>(len);

    for (int i = 0; i < len; i++) {
      long nodeId = buf.readLong();
      int numNeighbor = buf.readInt();
      long[] nbrs = new long[numNeighbor];
      float[] weight = new float[numNeighbor];

      for (int j = 0; j < numNeighbor; j++) {
        nbrs[j] = buf.readLong();
      }
      for (int j = 0; j < numNeighbor; j++) {
        weight[j] = buf.readFloat();
      }
      NeighborsRandomTableElement nbrElem = new NeighborsRandomTableElement(nbrs, weight);
      nodeId2Neighbors.put(nodeId, nbrElem);
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4;
    for (int i = startIndex; i < endIndex; i++) {
      NeighborsRandomTableElement nbrElem = nodeId2Neighbors.get(nodeIds[i]);
      if (nbrElem != null) {
        int numNodes = nbrElem.getNodesNum();
        len += 12;//nodeId and numNodes
        len += Long.BYTES * numNodes + Float.BYTES * numNodes;
      }
    }

    return len;
  }
}
