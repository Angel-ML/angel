package com.tencent.angel.graph.client.sampleneighborfeature;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import io.netty.buffer.ByteBuf;
import java.util.Random;

public class SampleEdgeFeaturePartResult extends PartitionGetResult {

  ServerLongAnyRow row;
  long[] keys;
  int numSample;
  int edgeDim;
  boolean sampleTypes;
  private int partId;
  private int[] indptr;
  private long[] neighbors;
  private IntFloatVector[] edgeFeatures;
  private int[] types;

  public SampleEdgeFeaturePartResult(int partId, ServerLongAnyRow row,
      long[] keys, int numSample, int edgeDim, boolean sampleTypes) {
    this.partId = partId;
    this.row = row;
    this.keys = keys;
    this.numSample = numSample;
    this.edgeDim = edgeDim;
    this.sampleTypes = sampleTypes;
  }

  public SampleEdgeFeaturePartResult() {

  }

  public int[] getIndptr() {
    return indptr;
  }

  public long[] getNeighbors() {
    return neighbors;
  }

  public IntFloatVector[] getEdgeFeatures() {
    return edgeFeatures;
  }

  public int[] getTypes() {
    return types;
  }

  public int getPartId() {
    return partId;
  }


  @Override
  public void serialize(ByteBuf buf) {
    // sample happens here to avoid memory copy on servers
    Random rand = new Random(System.currentTimeMillis());

    buf.writeInt(partId); // write partition id first
    buf.writeBoolean(sampleTypes);
    buf.writeInt(keys.length);
    int writeIndex = buf.writerIndex();
    buf.writeInt(0);

    int length = 0;
    for (int i = 0; i < keys.length; i++) {
      Node node = (Node) row.get(keys[i]);
      if (node == null) {
        buf.writeInt(0); // size for this node
        continue;
      }

      long[] neighbor = node.getNeighbors();
      int[] types = node.getTypes();
      IntFloatVector[] edgeFeature = node.getEdgeFeatures();

      if (neighbor == null || neighbor.length == 0) {
        buf.writeInt(0);
        continue;
      }

      int size = numSample;
      if (numSample <= 0 || numSample >= neighbor.length) {
        size = neighbor.length;
      }

      length += size; // # neighbors/types
      buf.writeInt(size);

      int start = rand.nextInt(neighbor.length);
      if (sampleTypes) {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          buf.writeLong(neighbor[idx]);
          NodeUtils.serialize(edgeFeature[idx], buf);
          buf.writeInt(types[idx]);
        }
      } else {
        for (int j = 0; j < size; j++) {
          int idx = (start + j) % neighbor.length;
          buf.writeLong(neighbor[idx]);
          NodeUtils.serialize(edgeFeature[idx], buf);
        }
      }
    }

    buf.setInt(writeIndex, length);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    partId = buf.readInt();
    sampleTypes = buf.readBoolean();
    int keysLen = buf.readInt();
    int neighborLen = buf.readInt();

    indptr = new int[keysLen + 1];
    neighbors = new long[neighborLen];
    edgeFeatures = new IntFloatVector[neighborLen];
    if (sampleTypes) {
      types = new int[neighborLen];
    }

    int idx1 = 0, idx2 = 0;
    indptr[idx1++] = 0;
    if (sampleTypes) {
      for (int i = 0; i < keysLen; i++) {
        int size = buf.readInt();
        for (int j = 0; j < size; j++) {
          neighbors[idx2] = buf.readLong();
          edgeFeatures[idx2] = NodeUtils.deserialize(buf);
          types[idx2] = buf.readInt();
          idx2++;
        }
        indptr[idx1++] = idx2;
      }
    } else {
      for (int i = 0; i < keysLen; i++) {
        int size = buf.readInt();
        for (int j = 0; j < size; j++) {
          neighbors[idx2] = buf.readLong();
          edgeFeatures[idx2] = NodeUtils.deserialize(buf);
          idx2++;
        }
        indptr[idx1++] = idx2;
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = 4 + 4 + 4 + ByteBufSerdeUtils.serializedBooleanLen(sampleTypes);
    for (int i = 0; i < keys.length; i++) {
      Node node = (Node) row.get(keys[i]);
      if (node == null) {
        len += 4;
        continue;
      }
      long[] neighbor = node.getNeighbors();
      IntFloatVector[] edgeFeature = node.getEdgeFeatures();
      if (neighbor == null || neighbor.length == 0) {
        len += 4;
        continue;
      }

      int size = numSample;
      if (numSample <= 0 || numSample >= neighbor.length) {
        size = neighbor.length;
      }

      len += 4;
      for (int j = 0; j < size; j++) {
        int idx = j % neighbor.length;
        len += 8 + NodeUtils.dataLen(edgeFeature[idx]);
      }
    }

    if (sampleTypes) {
      len += types.length * 4;
    }

    return len;
  }
}
