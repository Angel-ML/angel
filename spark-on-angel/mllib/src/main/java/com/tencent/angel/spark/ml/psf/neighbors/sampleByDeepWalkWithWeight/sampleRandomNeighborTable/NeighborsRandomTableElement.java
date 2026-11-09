package com.tencent.angel.spark.ml.psf.neighbors.sampleByDeepWalkWithWeight.sampleRandomNeighborTable;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (neighbors list with weight)
 */
public class NeighborsRandomTableElement implements IElement {

  private long[] neighborIds;
  private float[] weights;

  public NeighborsRandomTableElement() {
    this(null, null);
  }

  public NeighborsRandomTableElement(long[] neighborIds, float[] weights) {
    this.neighborIds = neighborIds;
    this.weights = weights;
  }

  public long[] getNeighborIds() {
    return neighborIds;
  }

  public float[] getWeights() {
    return weights;
  }

  public int getNodesNum() {
    return neighborIds.length;
  }

  @Override
  public Object deepClone() {
    int len = neighborIds.length;
    long[] newNodeIds = new long[len];
    float[] newWeights = new float[len];

    System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
    System.arraycopy(weights, 0, newWeights, 0, len);
    return new NeighborsRandomTableElement(newNodeIds, newWeights);
  }

  @Override
  public void serialize(ByteBuf output) {
    int len = neighborIds.length;
    output.writeInt(len);

    for (int i = 0; i < len; i++) {
      output.writeLong(neighborIds[i]);
    }

    for (int i = 0; i < len; i++) {
      output.writeFloat(weights[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    neighborIds = new long[len];
    weights = new float[len];

    for (int i = 0; i < len; i++) {
      neighborIds[i] = input.readLong();
    }

    for (int i = 0; i < len; i++) {
      weights[i] = input.readFloat();
    }
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    int len = neighborIds.length;
    output.writeInt(len);
    for (int i = 0; i < len; i++) {
      output.writeLong(neighborIds[i]);
    }
    for (int i = 0; i < len; i++) {
      output.writeFloat(weights[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    int len = input.readInt();
    neighborIds = new long[len];
    weights = new float[len];

    for (int i = 0; i < len; i++) {
      neighborIds[i] = input.readLong();
    }

    for (int i = 0; i < len; i++) {
      weights[i] = input.readFloat();
    }
  }

  @Override
  public int bufferLen() {
    return Integer.BYTES + neighborIds.length * Long.BYTES + weights.length * Float.BYTES;
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
