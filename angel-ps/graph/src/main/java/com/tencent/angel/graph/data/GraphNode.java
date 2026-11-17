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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.graph.client.constent.Constent;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Graph Node Base Data Struct: node contains features, neighbors, node types, edge types, edge
 * features, edge weights and node labels.
 */
public class GraphNode implements IElement {

  private ByteArrayElement feats; // node's feature
  private FeatureKeyType keyType;
  private Neighbor neighbors; // node's neighbors
  private int[] types; // types of neighbors (include the node itself)
  private int[] edgeTypes; // types of edges
  private Feature[] edgeFeatures; // edges' features
  private float[] weights; // edges' weights
  private float[] labels; // node or edges' labels, for multi-label node classification or edge classification separately
  private AliasTable aliasTable; // alias table for weighted sampling
  private long[] trainEdges;
  private long[] validEdges;
  private int[] trainEdgeLabels;
  private int[] validEdgeLabels;
  private Int2ObjectOpenHashMap<long[]> typeNeighbors;
  private int[] edgeTimestamps;

  public GraphNode(ByteArrayElement feats, Neighbor neighbors, int[] types, int[] edgeTypes,
      Feature[] edgeFeatures, float[] weights, float[] labels) {
    this.feats = feats;
    this.neighbors = neighbors;
    this.types = types;
    this.edgeTypes = edgeTypes;
    this.edgeFeatures = edgeFeatures;
    this.weights = weights;
    this.labels = labels;
  }

  public GraphNode(ByteArrayElement feats, Neighbor neighbors, int[] types, int[] edgeTypes,
      Feature[] edgeFeatures, float[] weights, float[] labels, AliasTable aliasTable) {
    this(feats, neighbors, types, edgeTypes, edgeFeatures, weights, labels);
    this.aliasTable = aliasTable;
  }

  public GraphNode(ByteArrayElement feats, Neighbor neighbors, int[] types, int[] edgeTypes,
      Feature[] edgeFeatures, float[] weights, float[] labels, AliasTable aliasTable,
      long[] trainEdges, long[] validEdges, int[] trainEdgeLabels, int[] validEdgeLabels) {
    this(feats, neighbors, types, edgeTypes, edgeFeatures, weights, labels, aliasTable);
    this.trainEdges = trainEdges;
    this.validEdges = validEdges;
    this.trainEdgeLabels = trainEdgeLabels;
    this.validEdgeLabels = validEdgeLabels;
  }

  public GraphNode(ByteArrayElement feats, Neighbor neighbors, int[] types, int[] edgeTypes,
                   Feature[] edgeFeatures, float[] weights, float[] labels, AliasTable aliasTable,
                   long[] trainEdges, long[] validEdges, int[] trainEdgeLabels, int[] validEdgeLabels,
                   Int2ObjectOpenHashMap<long []> typeNeighbors) {
    this(feats, neighbors, types, edgeTypes, edgeFeatures, weights, labels, aliasTable);
    this.trainEdges = trainEdges;
    this.validEdges = validEdges;
    this.trainEdgeLabels = trainEdgeLabels;
    this.validEdgeLabels = validEdgeLabels;
    this.typeNeighbors = typeNeighbors;
  }

  public GraphNode(ByteArrayElement feats, Neighbor neighbors, int[] types, int[] edgeTypes,
      Feature[] edgeFeatures, float[] weights, float[] labels, AliasTable aliasTable,
      long[] trainEdges, long[] validEdges, int[] trainEdgeLabels, int[] validEdgeLabels,
      int[] edgeTimestamps) {
    this(feats, neighbors, types, edgeTypes, edgeFeatures, weights, labels, aliasTable,
        trainEdges, validEdges, trainEdgeLabels, validEdgeLabels);
    this.edgeTimestamps = edgeTimestamps;
  }

  public GraphNode(ByteArrayElement feats, Neighbor neighbors) {
    this(feats, neighbors, null, null,
        null, null, null);
  }

  public GraphNode() {
    this(null, null, null, null,
        null, null, null);
  }

  public FeatureKeyType getKeyType() {
    return keyType;
  }

  public void setKeyType(FeatureKeyType keyType) {
    this.keyType = keyType;
  }

  public ByteArrayElement getFeats() {
    return feats;
  }

  public void setFeats(ByteArrayElement feats, FeatureKeyType keyType) {
    this.feats = feats;
    this.keyType = keyType;
  }

  public Neighbor getNeighbors() {
    return neighbors;
  }

  public void setNeighbors(Neighbor neighbors) {
    this.neighbors = neighbors;
  }

  public int[] getTypes() {
    return types;
  }

  public void setTypes(int[] types) {
    this.types = types;
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(int[] edgeTypes) {
    this.edgeTypes = edgeTypes;
  }

  public Int2ObjectOpenHashMap<long[]> getTypeNeighbors() {
    return this.typeNeighbors;
  }

  public void setTypeNeighbors(Int2ObjectOpenHashMap<long[]> typeNeighbors) { this.typeNeighbors = typeNeighbors; }

  public Feature[] getEdgeFeatures() {
    return edgeFeatures;
  }

  public void setEdgeFeatures(Feature[] edgeFeatures) {
    this.edgeFeatures = edgeFeatures;
  }

  public float[] getWeights() {
    return weights;
  }

  public void setWeights(float[] weights) {
    this.weights = weights;
  }

  public float getWeight(int index) {
    return weights[index];
  }

  public float[] getLabels() {
    return labels;
  }

  public void setLabels(float[] labels) {
    this.labels = labels;
  }

  public AliasTable getAliasTable() {
    return aliasTable;
  }

  public void setAliasTable(AliasTable aliasTable) {
    this.aliasTable = aliasTable;
  }

  public long[] getTrainEdges() {
    return trainEdges;
  }

  public void setTrainEdges(long[] trainEdges) {
    this.trainEdges = trainEdges;
  }

  public long[] getValidEdges() {
    return validEdges;
  }

  public void setValidEdges(long[] validEdges) {
    this.validEdges = validEdges;
  }

  public int[] getTrainEdgeLabels() {
    return trainEdgeLabels;
  }

  public void setTrainEdgeLabels(int[] trainEdgeLabels) {
    this.trainEdgeLabels = trainEdgeLabels;
  }

  public int[] getValidEdgeLabels() {
    return validEdgeLabels;
  }

  public void setValidEdgeLabels(int[] validEdgeLabels) {
    this.validEdgeLabels = validEdgeLabels;
  }

  public int[] getEdgeTimestamps() {
    return edgeTimestamps;
  }

  public void setEdgeTimestamps(int[] edgeTimestamps) {
    this.edgeTimestamps = edgeTimestamps;
  }

  @Override
  public GraphNode deepClone() {
    ByteArrayElement cloneFeats = null;
    if (feats != null) {
      cloneFeats = feats.deepClone();
    }

    Neighbor cloneNeighbors = null;
    if (neighbors != null) {
      cloneNeighbors = (Neighbor)neighbors.deepClone();
    }

    int[] cloneTypes = null;
    if (types != null) {
      cloneTypes = new int[types.length];
      System.arraycopy(types, 0, cloneTypes, 0, types.length);
    }

    int[] cloneEdgeTypes = null;
    if (edgeTypes != null) {
      cloneEdgeTypes = new int[edgeTypes.length];
      System.arraycopy(edgeTypes, 0, cloneEdgeTypes, 0, edgeTypes.length);
    }

    Feature[] cloneEdgeFeatures = null;
    if (edgeFeatures != null) {
      int edgeFeatsLen = edgeFeatures.length;
      cloneEdgeFeatures = new Feature[edgeFeatures.length];
      for (int i = 0; i < edgeFeatsLen; i++) {
        cloneEdgeFeatures[i] = (Feature) edgeFeatures[i].deepClone();
      }
    }

    float[] cloneWeights = null;
    if (weights != null) {
      cloneWeights = new float[weights.length];
      System.arraycopy(weights, 0, cloneWeights, 0, weights.length);
    }

    float[] cloneLables = null;
    if (labels != null) {
      cloneLables = new float[labels.length];
      System.arraycopy(labels, 0, cloneLables, 0, labels.length);
    }

    AliasTable cloneAliasTable = null;
    if (aliasTable != null) {
      cloneAliasTable = (AliasTable) aliasTable.deepClone();
    }

    long[] cloneTrainEdges = null;
    if (trainEdges != null) {
      cloneTrainEdges = new long[trainEdges.length];
      System.arraycopy(trainEdges, 0, cloneTrainEdges, 0, trainEdges.length);
    }

    long[] cloneValidEdges = null;
    if (validEdges != null) {
      cloneValidEdges = new long[validEdges.length];
      System.arraycopy(validEdges, 0, cloneValidEdges, 0, validEdges.length);
    }

    int[] cloneTrainEdgeLabels = null;
    if (trainEdgeLabels != null) {
      cloneTrainEdgeLabels = new int[trainEdgeLabels.length];
      System.arraycopy(trainEdgeLabels, 0, cloneTrainEdges, 0, trainEdgeLabels.length);
    }

    int[] cloneValidEdgeLabels = null;
    if (validEdgeLabels != null) {
      cloneValidEdgeLabels = new int[validEdgeLabels.length];
      System.arraycopy(validEdgeLabels, 0, cloneValidEdgeLabels, 0, validEdgeLabels.length);
    }

    int[] cloneEdgeTimestamps = null;
    if (edgeTimestamps != null) {
      cloneEdgeTimestamps = new int[edgeTimestamps.length];
      System.arraycopy(edgeTimestamps, 0, cloneEdgeTimestamps, 0, edgeTimestamps.length);
    }

    GraphNode node = new GraphNode(cloneFeats, cloneNeighbors, cloneTypes, cloneEdgeTypes,
            cloneEdgeFeatures, cloneWeights, cloneLables, cloneAliasTable,
            cloneTrainEdges, cloneValidEdges, cloneTrainEdgeLabels, cloneValidEdgeLabels, cloneEdgeTimestamps);
    node.setKeyType(keyType);
    return node;
  }

  @Override
  public void serialize(ByteBuf output) {
    if (feats != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeByte(output, keyType.getIndex());
      feats.serialize(output);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (neighbors != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeByte(output, neighbors.getStorageType().getIndex());
      neighbors.serialize(output);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (types != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeInts(output, types);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (edgeTypes != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeInts(output, edgeTypes);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (edgeFeatures != null) {
      ByteBufSerdeUtils.serializeInt(output, edgeFeatures.length);
      for (Feature edgeFeature : edgeFeatures) {
        edgeFeature.serialize(output);
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }

    if (weights != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeFloats(output, weights);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (labels != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeFloats(output, labels);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (aliasTable != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      aliasTable.serialize(output);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (trainEdges != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeLongs(output, trainEdges);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (validEdges != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeLongs(output, validEdges);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (trainEdgeLabels != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeInts(output, trainEdgeLabels);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (validEdgeLabels != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeInts(output, validEdgeLabels);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (typeNeighbors != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeInt(output, typeNeighbors.size());
      for (Int2ObjectOpenHashMap.Entry<long[]> entry : typeNeighbors
              .int2ObjectEntrySet()) {
        ByteBufSerdeUtils.serializeInt(output, entry.getIntKey());
        long[] neighbors = entry.getValue();
        if (neighbors == null) {
          ByteBufSerdeUtils.serializeLongs(output, Constent.emptyLongs);
        } else {
          ByteBufSerdeUtils.serializeLongs(output, neighbors);
        }
      }
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (edgeTimestamps != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeInts(output, edgeTimestamps);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }
  }

  @Override
  public void deserialize(ByteBuf input) {
    boolean featsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (featsFlag) {
      keyType = FeatureKeyType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
      feats = new ByteArrayElement();
      feats.deserialize(input);
    }

    boolean neighborsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (neighborsFlag) {
      NeighborStorageType storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
      switch (storageType) {
        case LONGARRAY: neighbors = new LongNeighbor(); break;
        case INTARRAY: neighbors = new IntNeighbor(); break;
      }
      neighbors.deserialize(input);
    }

    boolean typesFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (typesFlag) {
      types = ByteBufSerdeUtils.deserializeInts(input);
    }

    boolean edgeTypesFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (edgeTypesFlag) {
      edgeTypes = ByteBufSerdeUtils.deserializeInts(input);
    }

    int edgeFeaturesLen = ByteBufSerdeUtils.deserializeInt(input);
    if (edgeFeaturesLen > 0) {
      edgeFeatures = new Feature[edgeFeaturesLen];
      for (int i = 0; i < edgeFeaturesLen; i++) {
        edgeFeatures[i] = new Feature();
        edgeFeatures[i].deserialize(input);
      }
    }

    boolean weightsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (weightsFlag) {
      weights = ByteBufSerdeUtils.deserializeFloats(input);
    }

    boolean labelsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (labelsFlag) {
      labels = ByteBufSerdeUtils.deserializeFloats(input);
    }

    boolean aliasTableFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (aliasTableFlag) {
      aliasTable = (AliasTable) ByteBufSerdeUtils.deserializeObject(input);
    }

    boolean trainEdgesFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (trainEdgesFlag) {
      trainEdges = ByteBufSerdeUtils.deserializeLongs(input);
    }

    boolean validEdgesFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (validEdgesFlag) {
      validEdges = ByteBufSerdeUtils.deserializeLongs(input);
    }

    boolean trainEdgeLabelsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (trainEdgeLabelsFlag) {
      trainEdgeLabels = ByteBufSerdeUtils.deserializeInts(input);
    }

    boolean validEdgeLabelsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (validEdgeLabelsFlag) {
      validEdgeLabels = ByteBufSerdeUtils.deserializeInts(input);
    }

    boolean typeNeighborsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (typeNeighborsFlag) {
      int size = ByteBufSerdeUtils.deserializeInt(input);
      typeNeighbors = new Int2ObjectOpenHashMap<>(size);
      for (int i = 0; i < size; i++) {
        int nodeId = ByteBufSerdeUtils.deserializeInt(input);
        long[] neighbors = ByteBufSerdeUtils.deserializeLongs(input);
        typeNeighbors.put(nodeId, neighbors);
      }
    }

    boolean edgeTimestampsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (edgeTimestampsFlag) {
      edgeTimestamps = ByteBufSerdeUtils.deserializeInts(input);
    }
  }

  @Override
  public int bufferLen() {
    // flags len for init value: 7*4
    int len = 13 * ByteBufSerdeUtils.BOOLEN_LENGTH;
    if (feats != null) {
      len += feats.bufferLen() + ByteBufSerdeUtils.BYTE_LENGTH; // feats buffer len
    }
    if (neighbors != null) {
      len += ByteBufSerdeUtils.BYTE_LENGTH;
      len += neighbors.bufferLen();
    }
    if (types != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(types);
    }
    if (edgeTypes != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(edgeTypes);
    }
    if (edgeFeatures != null && edgeFeatures[0] != null) {
      len += ByteBufSerdeUtils.INT_LENGTH;
      for (Feature edgeFeature : edgeFeatures) {
        len += edgeFeature.bufferLen();
      }
    }
    if (weights != null) {
      len += ByteBufSerdeUtils.serializedFloatsLen(weights);
    }
    if (labels != null) {
      len += ByteBufSerdeUtils.serializedFloatsLen(labels);
    }
    if (aliasTable != null) {
      len += aliasTable.bufferLen();
    }
    if (trainEdges != null) {
      len += ByteBufSerdeUtils.serializedLongsLen(trainEdges);
    }
    if (validEdges != null) {
      len += ByteBufSerdeUtils.serializedLongsLen(validEdges);
    }
    if (trainEdgeLabels != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(trainEdgeLabels);
    }
    if (validEdgeLabels != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(validEdgeLabels);
    }
    if(typeNeighbors != null) {
      len += ByteBufSerdeUtils.INT_LENGTH;
      for (Int2ObjectOpenHashMap.Entry<long[]> entry : typeNeighbors
              .int2ObjectEntrySet()) {
        len += ByteBufSerdeUtils.INT_LENGTH;
        long[] neighbors = entry.getValue();
        if (neighbors == null) {
          len += ByteBufSerdeUtils.serializedLongsLen(Constent.emptyLongs);
        } else {
          len += ByteBufSerdeUtils.serializedLongsLen(neighbors);
        }
      }
    }
    if (edgeTimestamps != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(edgeTimestamps);
    }
    return len;
  }

  @Override
  public void serialize(DataOutputStream output) throws IOException {
    if (feats != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeByte(output, keyType.getIndex());
      feats.serialize(output);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (neighbors != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeByte(output, neighbors.getStorageType().getIndex());
      neighbors.serialize(output);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (types != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeInts(output, types);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (edgeTypes != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeInts(output, edgeTypes);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (edgeFeatures != null) {
      StreamSerdeUtils.serializeInt(output, edgeFeatures.length);
      for (Feature edgeFeature : edgeFeatures) {
        edgeFeature.serialize(output);
      }
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }

    if (weights != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeFloats(output, weights);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (labels != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeFloats(output, labels);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (aliasTable != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      aliasTable.serialize(output);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (trainEdges != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeLongs(output, trainEdges);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (validEdges != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeLongs(output, validEdges);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (trainEdgeLabels != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeInts(output, trainEdgeLabels);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (validEdgeLabels != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeInts(output, validEdgeLabels);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (typeNeighbors != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeInt(output, typeNeighbors.size());
      for (Int2ObjectOpenHashMap.Entry<long[]> entry : typeNeighbors
              .int2ObjectEntrySet()) {
        StreamSerdeUtils.serializeInt(output, entry.getIntKey());
        long[] neighbors = entry.getValue();
        if (neighbors == null) {
          StreamSerdeUtils.serializeLongs(output, Constent.emptyLongs);
        } else {
          StreamSerdeUtils.serializeLongs(output, neighbors);
        }
      }
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (edgeTimestamps != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeInts(output, edgeTimestamps);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }
  }

  @Override
  public void deserialize(DataInputStream input) throws IOException{
    boolean featsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (featsFlag) {
      keyType = FeatureKeyType.valueOf(StreamSerdeUtils.deserializeByte(input));
      feats = new ByteArrayElement();
      feats.deserialize(input);
    }

    boolean neighborsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (neighborsFlag) {
      NeighborStorageType storageType = NeighborStorageType.valueOf(StreamSerdeUtils.deserializeByte(input));
      switch (storageType) {
        case LONGARRAY: neighbors = new LongNeighbor(); break;
        case INTARRAY: neighbors = new IntNeighbor(); break;
      }
      neighbors.deserialize(input);
    }

    boolean typesFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (typesFlag) {
      types = StreamSerdeUtils.deserializeInts(input);
    }

    boolean edgeTypesFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (edgeTypesFlag) {
      edgeTypes = StreamSerdeUtils.deserializeInts(input);
    }

    int edgeFeaturesLen = StreamSerdeUtils.deserializeInt(input);
    if (edgeFeaturesLen > 0) {
      edgeFeatures = new Feature[edgeFeaturesLen];
      for (int i = 0; i < edgeFeaturesLen; i++) {
        edgeFeatures[i].deserialize(input);
      }
    }

    boolean weightsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (weightsFlag) {
      weights = StreamSerdeUtils.deserializeFloats(input);
    }

    boolean labelsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (labelsFlag) {
      labels = StreamSerdeUtils.deserializeFloats(input);
    }

    boolean aliasTableFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (aliasTableFlag) {
      aliasTable = (AliasTable) StreamSerdeUtils.deserializeObject(input);
    }

    boolean trainEdgesFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (trainEdgesFlag) {
      trainEdges = StreamSerdeUtils.deserializeLongs(input);
    }

    boolean validEdgesFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (validEdgesFlag) {
      validEdges = StreamSerdeUtils.deserializeLongs(input);
    }

    boolean trainEdgeLabelsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (trainEdgeLabelsFlag) {
      trainEdgeLabels = StreamSerdeUtils.deserializeInts(input);
    }

    boolean validEdgeLabelsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (validEdgeLabelsFlag) {
      validEdgeLabels = StreamSerdeUtils.deserializeInts(input);
    }

    boolean typeNeighborsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (typeNeighborsFlag) {
      int size = StreamSerdeUtils.deserializeInt(input);
      typeNeighbors = new Int2ObjectOpenHashMap<>(size);
      for (int i = 0; i < size; i++) {
        int nodeId = StreamSerdeUtils.deserializeInt(input);
        long[] neighbors = StreamSerdeUtils.deserializeLongs(input);
        typeNeighbors.put(nodeId, neighbors);
      }
    }

    boolean edgeTimestampsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (edgeTimestampsFlag) {
      edgeTimestamps = StreamSerdeUtils.deserializeInts(input);
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }
}