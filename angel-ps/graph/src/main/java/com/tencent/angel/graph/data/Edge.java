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
    return null;
  }

  @Override
  public void serialize(ByteBuf output) {

  }

  @Override
  public void deserialize(ByteBuf input) {

  }

  @Override
  public int bufferLen() {
    return 0;
  }
}
