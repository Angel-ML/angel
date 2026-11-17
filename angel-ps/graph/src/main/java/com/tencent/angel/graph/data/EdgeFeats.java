package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class EdgeFeats implements IElement {

  private Neighbor neighbors;
  private Feature[] feats;

  public EdgeFeats(Neighbor neighbors, Feature[] feats) {
    this.neighbors = neighbors;
    this.feats = feats;
  }

  public EdgeFeats() {
    this(null, null);
  }

  public Feature[] getFeats() {
    return feats;
  }

  public Neighbor getNeighbors() {
    return neighbors;
  }

  @Override
  public void serialize(ByteBuf output) {
    ByteBufSerdeUtils.serializeByte(output, neighbors.getStorageType().getIndex());
    neighbors.serialize(output);
    ByteBufSerdeUtils.serializeInt(output, feats.length);
    for (Feature edgeFeats: feats) {
      edgeFeats.serialize(output);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    NeighborStorageType storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
    if (storageType == NeighborStorageType.LONGARRAY) {
      neighbors = new LongNeighbor();
    } else {
      neighbors = new IntNeighbor();
    }
    neighbors.deserialize(input);
    int len = ByteBufSerdeUtils.deserializeInt(input);
    feats = new Feature[len];
    for (int i = 0; i < feats.length; i++) {
      feats[i] = new Feature();
      feats[i].deserialize(input);
    }
  }

  @Override
  public int bufferLen() {
    int len = ByteBufSerdeUtils.BYTE_LENGTH + neighbors.bufferLen();
    len += ByteBufSerdeUtils.INT_LENGTH;
    for (Feature edgeFeats: feats) {
      len += edgeFeats.bufferLen();
    }
    return len;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    StreamSerdeUtils.serializeByte(output, neighbors.getStorageType().getIndex());
    neighbors.serialize(output);
    StreamSerdeUtils.serializeInt(output, feats.length);
    for (Feature edgeFeats: feats) {
      edgeFeats.serialize(output);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException {
    NeighborStorageType storageType = NeighborStorageType.valueOf(StreamSerdeUtils.deserializeByte(input));
    if (storageType == NeighborStorageType.LONGARRAY) {
      neighbors = new LongNeighbor();
    } else {
      neighbors = new IntNeighbor();
    }
    neighbors.deserialize(input);
    int len = StreamSerdeUtils.deserializeInt(input);
    feats = new Feature[len];
    for (int i = 0; i < feats.length; i++) {
      feats[i] = new Feature();
      feats[i].deserialize(input);
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  @Override
  public EdgeFeats deepClone() {
    Neighbor cloneNeighbors = null;
    if (neighbors != null) {
      cloneNeighbors = (Neighbor)neighbors.deepClone();
    }
    Feature[] cloneFeats = null;
    if (feats != null) {
      cloneFeats = new Feature[feats.length];
      for (int i = 0; i < feats.length; i++) {
        cloneFeats[i] = (Feature) feats[i].deepClone();
      }
    }

    return new EdgeFeats(cloneNeighbors, cloneFeats);
  }
}
