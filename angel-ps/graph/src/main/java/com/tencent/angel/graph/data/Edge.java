/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.graph.data;

import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;

import java.util.Arrays;

public class Edge implements IEdge, IElement {
  EdgeId id;
  int type;
  float weight;
  int [] longFeatureIndices;
  long [] longFeatures;
  int [] floatFeatureIndices;
  float [] floatFeatures;
  int [] binaryFeatureIndices;
  byte [] binaryFeatures;

  public Edge(EdgeId id, int type, float weight,  int[] longFeatureIndices, long[] longFeatures,
      int[] floatFeatureIndices, float[] floatFeatures, int[] binaryFeatureIndices,
      byte[] binaryFeatures) {
    this.id = id;
    this.type = type;
    this.weight = weight;
    this.longFeatureIndices = longFeatureIndices;
    this.longFeatures = longFeatures;
    this.floatFeatureIndices = floatFeatureIndices;
    this.floatFeatures = floatFeatures;
    this.binaryFeatureIndices = binaryFeatureIndices;
    this.binaryFeatures = binaryFeatures;
  }

  public Edge() {
  }

  public EdgeId getId() {
    return id;
  }

  public void setId(EdgeId id) {
    this.id = id;
  }

  public int getType() {
    return type;
  }

  public void setType(int type) {
    this.type = type;
  }

  public float getWeight() {
    return weight;
  }

  public void setWeight(float weight) {
    this.weight = weight;
  }

  public int[] getLongFeatureIndices() {
    return longFeatureIndices;
  }

  public void setLongFeatureIndices(int[] longFeatureIndices) {
    this.longFeatureIndices = longFeatureIndices;
  }

  public long[] getLongFeatures() {
    return longFeatures;
  }

  public void setLongFeatures(long[] longFeatures) {
    this.longFeatures = longFeatures;
  }

  public int[] getFloatFeatureIndices() {
    return floatFeatureIndices;
  }

  public void setFloatFeatureIndices(int[] floatFeatureIndices) {
    this.floatFeatureIndices = floatFeatureIndices;
  }

  public float[] getFloatFeatures() {
    return floatFeatures;
  }

  public void setFloatFeatures(float[] floatFeatures) {
    this.floatFeatures = floatFeatures;
  }

  public int[] getBinaryFeatureIndices() {
    return binaryFeatureIndices;
  }

  public void setBinaryFeatureIndices(int[] binaryFeatureIndices) {
    this.binaryFeatureIndices = binaryFeatureIndices;
  }

  public byte[] getBinaryFeatures() {
    return binaryFeatures;
  }

  public void setBinaryFeatures(byte[] binaryFeatures) {
    this.binaryFeatures = binaryFeatures;
  }


  @Override
  public Object deepClone() {
    EdgeId edgeIdClone = new EdgeId(id.getFromNodeId(), id.getToNodeId(), id.getType());
    int[] longFeatureIndicesClone = Arrays.copyOf(longFeatureIndices, longFeatureIndices.length);
    long[] longFeaturesClone = Arrays.copyOf(longFeatures, longFeatures.length);
    int[] floatFeatureIndicesClone = Arrays.copyOf(floatFeatureIndices, floatFeatureIndices.length);
    float[] floatFeaturesClone = Arrays.copyOf(floatFeatures, floatFeatures.length);
    int[] binaryFeatureIndicesClone = Arrays.copyOf(binaryFeatureIndices, binaryFeatureIndices.length);
    byte[] binaryFeaturesClone = Arrays.copyOf(binaryFeatures, binaryFeatures.length);

    return new Edge(edgeIdClone, type, weight,
      longFeatureIndicesClone, longFeaturesClone, floatFeatureIndicesClone, floatFeaturesClone,
      binaryFeatureIndicesClone, binaryFeaturesClone);
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeLong(id.getFromNodeId());

    buf.writeLong(id.getToNodeId());

    buf.writeInt(id.getType());

    buf.writeFloat(weight);

    buf.writeInt(longFeatureIndices.length);

    for (int longFeatureIndex : longFeatureIndices) {
      buf.writeInt(longFeatureIndex);
    }

    buf.writeInt(longFeatures.length);

    for (long longFeature : longFeatures) {
      buf.writeLong(longFeature);
    }

    buf.writeInt(floatFeatureIndices.length);

    for (int floatFeatureIndex : floatFeatureIndices) {
      buf.writeInt(floatFeatureIndex);
    }

    buf.writeInt(floatFeatures.length);

    for (float floatFeature : floatFeatures) {
      buf.writeFloat(floatFeature);
    }

    buf.writeInt(binaryFeatureIndices.length);

    for (int binaryFeatureIndex : binaryFeatureIndices) {
      buf.writeInt(binaryFeatureIndex);
    }

    buf.writeInt(binaryFeatures.length);

    buf.writeBytes(binaryFeatures);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    long fromNodeId = buf.readLong();

    long toNodeId = buf.readLong();

    type = buf.readInt();

    id = new EdgeId(fromNodeId, toNodeId, type);

    weight = buf.readFloat();

    int longFeatureNum = buf.readInt();

    longFeatureIndices = new int[longFeatureNum];
    for (int i = 0; i < longFeatureNum; i++) {
      longFeatureIndices[i] = buf.readInt();
    }

    int longFeatureValueNum = buf.readInt();

    longFeatures = new long[longFeatureValueNum];
    for (int i = 0; i < longFeatureValueNum; i++) {
      longFeatures[i] = buf.readLong();
    }

    int floatFeatureNum = buf.readInt();

    floatFeatureIndices = new int[floatFeatureNum];
    for (int i = 0; i < floatFeatureNum; i++) {
      floatFeatureIndices[i] = buf.readInt();
    }

    int floatFeatureValueNum = buf.readInt();

    floatFeatures = new float[floatFeatureValueNum];
    for (int i = 0; i < floatFeatureValueNum; i++) {
      floatFeatures[i] = buf.readFloat();
    }

    int binaryFeatureNum = buf.readInt();

    binaryFeatureIndices = new int[binaryFeatureNum];
    for (int i = 0; i < binaryFeatureNum; i++) {
      binaryFeatureIndices[i] = buf.readInt();
    }

    int binaryFeatureValueNum = buf.readInt();
    binaryFeatures = buf.readBytes(binaryFeatureValueNum).array();
  }

  @Override
  public int bufferLen() {
    return 24 + (8 + longFeatureIndices.length * 4 + longFeatures.length * 8) +
            (8 + floatFeatureIndices.length * 4 + floatFeatures.length * 4) +
            (8 + binaryFeatureIndices.length * 4 + binaryFeatures.length);
  }
}
