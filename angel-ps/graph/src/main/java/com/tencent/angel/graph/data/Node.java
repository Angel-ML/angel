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

/**
 * Graph node
 */
public class Node implements INode, IElement {

  /**
   * Node id
   */
  long id;

  /**
   * Node type
   */
  int type;

  /**
   * Node weight
   */
  float weight;

  /**
   * Edge types
   */
  int[] edgeTypes;

  /**
   * Accumulation sums of edge weights, dimension is equal to edgeTypes
   */
  float[] accSumWeights;

  /**
   * Total sum of edge weights
   */
  float totalSumWeights;

  /**
   * Neighbor group positions in "neighbors", each neighbor group corresponding to an edge type
   */
  int[] neigborGroupIndices;

  /**
   * Neighbor node ids of this node
   */
  long[] neighbors;

  /**
   * Accumulation sums of neighbor node weights
   */
  float[] neighborsWeight;

  /**
   * Long type feature indices
   */
  int[] longFeatureIndices;

  /**
   * Long feature values
   */
  long[] longFeatures;

  /**
   * Float feature indices
   */
  int[] floatFeatureIndices;

  /**
   * Float feature values
   */
  float[] floatFeatures;

  /**
   * Binary feature indices
   */
  int[] binaryFeatureIndices;

  /**
   * Binary feature values
   */
  String[] binaryFeatures;

  public Node(long id, int type, float weight, int[] edgeTypes, float[] accSumWeights,
      float totalSumWeights, int[] neigborGroupIndices, long[] neighbors, float[] neighborsWeight,
      int[] longFeatureIndices, long[] longFeatures, int[] floatFeatureIndices,
      float[] floatFeatures,
      int[] binaryFeatureIndices, String[] binaryFeatures) {
    this.id = id;
    this.type = type;
    this.weight = weight;
    this.edgeTypes = edgeTypes;
    this.accSumWeights = accSumWeights;
    this.totalSumWeights = totalSumWeights;
    this.neigborGroupIndices = neigborGroupIndices;
    this.neighbors = neighbors;
    this.neighborsWeight = neighborsWeight;
    this.longFeatureIndices = longFeatureIndices;
    this.longFeatures = longFeatures;
    this.floatFeatureIndices = floatFeatureIndices;
    this.floatFeatures = floatFeatures;
    this.binaryFeatureIndices = binaryFeatureIndices;
    this.binaryFeatures = binaryFeatures;
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
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

  public float[] getAccSumWeights() {
    return accSumWeights;
  }

  public void setAccSumWeights(float[] accSumWeights) {
    this.accSumWeights = accSumWeights;
  }

  public float getTotalSumWeights() {
    return totalSumWeights;
  }

  public void setTotalSumWeights(float totalSumWeights) {
    this.totalSumWeights = totalSumWeights;
  }

  public int[] getNeigborGroupIndices() {
    return neigborGroupIndices;
  }

  public void setNeigborGroupIndices(int[] neigborGroupIndices) {
    this.neigborGroupIndices = neigborGroupIndices;
  }

  public long[] getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(long[] neighbors) {
    this.neighbors = neighbors;
  }

  public float[] getNeighborsWeight() {
    return neighborsWeight;
  }

  public void setNeighborsWeight(float[] neighborsWeight) {
    this.neighborsWeight = neighborsWeight;
  }

  public int[] getLongFeatureIndices() {
    return longFeatureIndices;
  }

  public void setLongFeatureIndices(int[] longFeatureIndices) {
    this.longFeatureIndices = longFeatureIndices;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(int[] edgeTypes) {
    this.edgeTypes = edgeTypes;
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

  public String[] getBinaryFeatures() {
    return binaryFeatures;
  }

  public void setBinaryFeatures(String[] binaryFeatures) {
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
