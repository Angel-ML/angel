package com.tencent.angel.spark.ml.psf.solution.simpleByAliasTable.sampleAliasTable;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * ps ：node's data (neighbors, accept, alias)
 */
public class NeighborsAliasTableElement implements IElement {

  private int[] neighborIds;
  private float[] accept;
  private int[] alias;


  public NeighborsAliasTableElement() {
    this(null, null, null);
  }

  public NeighborsAliasTableElement(int[] neighborIds, float[] accept, int[] alias) {
    assert (neighborIds.length == accept.length && neighborIds.length == alias.length);
    this.neighborIds = neighborIds;
    this.alias = alias;
    this.accept = accept;
  }

  public int[] getNeighborIds() {
    return neighborIds;
  }

  public float[] getAccept() {
    return accept;
  }

  public int[] getAlias() {
    return alias;
  }

  public int getNodesNum() {
    return neighborIds.length;
  }

  @Override
  public Object deepClone() {
    int len = neighborIds.length;
    int[] newNodeIds = new int[len];
    int[] newAlias = new int[len];
    float[] newAccept = new float[len];

    System.arraycopy(neighborIds, 0, newNodeIds, 0, len);
    System.arraycopy(accept, 0, newAccept, 0, len);
    System.arraycopy(alias, 0, newAlias, 0, len);
    return new NeighborsAliasTableElement(newNodeIds, newAccept, newAlias);
  }

  @Override
  public void serialize(ByteBuf output) {
    int len = neighborIds.length;
    output.writeInt(len);
    for (int i = 0; i < len; i++) {
      output.writeInt(neighborIds[i]);
    }
    for (int i = 0; i < len; i++) {
      output.writeFloat(accept[i]);
    }
    for (int i = 0; i < len; i++) {
      output.writeInt(alias[i]);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    int len = input.readInt();
    neighborIds = new int[len];
    alias = new int[len];
    accept = new float[len];

    for (int i = 0; i < len; i++) {
      neighborIds[i] = input.readInt();
    }

    for (int i = 0; i < len; i++) {
      accept[i] = input.readFloat();
    }

    for (int i = 0; i < len; i++) {
      alias[i] = input.readInt();
    }
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    int len = neighborIds.length;
    output.writeInt(len);
    for (int i = 0; i < len; i++) {
      output.writeInt(neighborIds[i]);
    }
    for (int i = 0; i < len; i++) {
      output.writeFloat(accept[i]);
    }
    for (int i = 0; i < len; i++) {
      output.writeInt(alias[i]);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    int len = input.readInt();
    neighborIds = new int[len];
    accept = new float[len];
    alias = new int[len];

    for (int i = 0; i < len; i++) {
      neighborIds[i] = input.readInt();
    }

    for (int i = 0; i < len; i++) {
      accept[i] = input.readFloat();
    }

    for (int i = 0; i < len; i++) {
      alias[i] = input.readInt();
    }
  }

  @Override
  public int bufferLen() {
    return Integer.BYTES + neighborIds.length * Integer.BYTES + accept.length * Float.BYTES
        + alias.length * Integer.BYTES;
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
