package com.tencent.angel.graph.client.initedgefeatures;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.graph.data.NodeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

public class InitEdgeFeatsPartParam extends PartitionUpdateParam {

  private long[] keys;
  private int[] index;
  private int[] indptr;
  private long[] neighbors;
  private int[] types;
  private int edgeDim;
  private IntFloatVector[] feats;
  private int startIndex;
  private int endIndex;
  private long[][] neighborArrays;
  private IntFloatVector[][] featArrays;
  private int[][] typeArrays;


  public InitEdgeFeatsPartParam(int matrixId, PartitionKey pkay,
      long[] keys, int[] index, int[] indptr, long[] neighbors, int[] types,
      int edgeDim, IntFloatVector[] feats, int startIndex, int endIndex) {
    super(matrixId, pkay);
    this.keys = keys;
    this.index = index;
    this.indptr = indptr;
    this.neighbors = neighbors;
    this.types = types;
    this.edgeDim = edgeDim;
    this.feats = feats;
    this.startIndex = startIndex;
    this.endIndex = endIndex;
  }

  public InitEdgeFeatsPartParam() {
    this(0, null, null, null, null, null, null, 0, null, 0, 0);
  }

  public long[] getKeys() {
    return keys;
  }

  public long[][] getNeighborArrays() {
    return neighborArrays;
  }

  public IntFloatVector[][] getFeatArrays() {
    return featArrays;
  }

  public int[][] getTypeArrays() {
    return typeArrays;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (types != null) {
      buf.writeBoolean(true);
    } else {
      buf.writeBoolean(false);
    }

    buf.writeInt(endIndex - startIndex);
    for (int i = startIndex; i < endIndex; i++) {
      long key = keys[index[i]];
      int len = indptr[index[i] + 1] - indptr[index[i]];
      buf.writeLong(key);
      buf.writeInt(len);
      for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++) {
        buf.writeLong(neighbors[j]);
        NodeUtils.serialize(feats[j], buf);
      }
      if (types != null) {
        for (int j = indptr[index[i]]; j < indptr[index[i] + 1]; j++) {
          buf.writeInt(types[j]);
        }
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    boolean hasType = buf.readBoolean();

    int size = buf.readInt();
    keys = new long[size];
    neighborArrays = new long[size][];
    featArrays = new IntFloatVector[size][];

    if (hasType) {
      typeArrays = new int[size][];
    }

    for (int i = 0; i < size; i++) {
      long node = buf.readLong();
      keys[i] = node;
      int len = buf.readInt();
      long[] neighbors = new long[len];
      IntFloatVector[] edgeFeats = new IntFloatVector[len];
      for (int j = 0; j < len; j++) {
        neighbors[j] = buf.readLong();
        edgeFeats[j] = NodeUtils.deserialize(buf);
      }
      neighborArrays[i] = neighbors;
      featArrays[i] = edgeFeats;

      if (hasType) {
        int[] types = new int[len];
        for (int j = 0; j < len; j++) {
          types[j] = buf.readInt();
        }
        typeArrays[i] = types;
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    len += 4 + ByteBufSerdeUtils.serializedBooleanLen(true);
    for (int i = startIndex; i < endIndex; i++) {
      len += 12;
      int count = indptr[index[i] + 1] - indptr[index[i]];
      len += 8 * count;
      if (feats[index[i]] != null && feats[index[i]].getSize() != 0) {
        len += count * NodeUtils.dataLen(feats[index[i]]);
      }
      if (types != null) {
        len += 4 * count;
      }
    }
    return len;
  }

}
