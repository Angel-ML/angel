package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiGraphNode implements IElement {

  private ByteArrayElement feats; // node's feature
  private FeatureKeyType keyType;
  private Map<String, Neighbor> neighbors; // node's neighbors
  private Map<String, int[]> types; // types of neighbors (include the node itself)
  private int[] edgeTypes; // types of edges
  private Feature[] edgeFeatures; // edges' features
  private Map<String, float[]> weights; // edges' weights
  private float[] labels; // node or edges' labels, for multi-label node classification or edge classification separately
  private Map<String, AliasTable> aliasTables;// alias table for weighted sampling

  public MultiGraphNode(ByteArrayElement feats, Map<String, Neighbor> neighbors, Map<String, int[]> types, int[] edgeTypes,
      Feature[] edgeFeatures, Map<String, float[]> weights, float[] labels) {
    this.feats = feats;
    this.neighbors = neighbors;
    this.types = types;
    this.edgeTypes = edgeTypes;
    this.edgeFeatures = edgeFeatures;
    this.weights = weights;
    this.labels = labels;
  }

  public MultiGraphNode(ByteArrayElement feats, Map<String, Neighbor> neighbors, Map<String, int[]> types, int[] edgeTypes,
      Feature[] edgeFeatures, Map<String, float[]> weights, float[] labels, Map<String, AliasTable> aliasTables) {
    this(feats, neighbors, types, edgeTypes, edgeFeatures, weights, labels);
    this.aliasTables = aliasTables;
  }

  public MultiGraphNode(ByteArrayElement feats, Map<String, Neighbor> neighbors) {
    this(feats, neighbors, null, null,
        null, null, null);
  }

  public MultiGraphNode() {
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

  public Map<String, Neighbor> getNeighbors() {
    return neighbors;
  }

  public Neighbor getNeighbors(String name) {
    return neighbors.get(name);
  }

  public void setNeighbors(Map<String, Neighbor> neighbors) {
    this.neighbors = neighbors;
  }

  public void setNeighbors(String name, Neighbor neighs) {
    if (this.neighbors == null) {
      this.neighbors = new HashMap<>();
    }
    this.neighbors.put(name, neighs);
  }

  public Map<String, int[]> getTypes() {
    return types;
  }

  public int[] getTypes(String name) {
    return types.get(name);
  }

  public void setTypes(Map<String, int[]> types) {
    this.types = types;
  }

  public void setTypes(String name, int[] types) {
    this.types.put(name, types);
  }

  public int[] getEdgeTypes() {
    return edgeTypes;
  }

  public void setEdgeTypes(int[] edgeTypes) {
    this.edgeTypes = edgeTypes;
  }

  public Feature[] getEdgeFeatures() {
    return edgeFeatures;
  }

  public void setEdgeFeatures(Feature[] edgeFeatures) {
    this.edgeFeatures = edgeFeatures;
  }

  public Map<String, float[]> getWeights() {
    return weights;
  }

  public float[] getWeights(String name) {
    return weights.get(name);
  }

  public void setWeights(Map<String, float[]> weights) {
    this.weights = weights;
  }

  public void setWeights(String name, float[] w) {
    if (this.weights == null) {
      this.weights = new HashMap<>();
    }
    this.weights.put(name, w );
  }

  public float[] getLabels() {
    return labels;
  }

  public void setLabels(float[] labels) {
    this.labels = labels;
  }

  public Map<String, AliasTable> getAliasTables() {
    return aliasTables;
  }

  public void setAliasTables(Map<String, AliasTable> aliasTables) {
    this.aliasTables = aliasTables;
  }

  public AliasTable getAliasTables(String name) {
    return this.aliasTables.get(name);
  }

  public void setAliasTables(String name, AliasTable aliasTable) {
    if (this.aliasTables == null) {
      this.aliasTables = new HashMap<>();
    }
    this.aliasTables.put(name, aliasTable);
  }

  @Override
  public MultiGraphNode deepClone() {
    ByteArrayElement cloneFeats = null;
    if (feats != null) {
      cloneFeats =feats.deepClone();
    }

    Map<String, Neighbor> cloneNeighbors = null;
   Neighbor cNeigh = null;
    if (neighbors != null) {
      cloneNeighbors = new HashMap();
      for (Map.Entry<String, Neighbor> neigh : neighbors.entrySet()) {
        cNeigh = (Neighbor)neigh.getValue().deepClone();
        cloneNeighbors.put(neigh.getKey(), cNeigh);
      }

    }

    Map<String, int[]> cloneTypes = null;
    int[] cType = null;
    if (types != null) {
      cloneTypes = new HashMap();
      for (Map.Entry<String, int[]> tpe : types.entrySet()) {
        cType = new int[tpe.getValue().length];
        System.arraycopy(tpe.getValue(), 0, cType, 0, tpe.getValue().length);
        cloneTypes.put(tpe.getKey(), cType);
      }
    }

    int[] cloneEdgeTypes = null;
    if (edgeTypes != null) {
      cloneEdgeTypes = new int[edgeTypes.length];
      System.arraycopy(edgeTypes, 0, cloneEdgeTypes, 0, edgeTypes.length);
    }

    Feature[] cloneEdgeFeatures = null;
    if (edgeFeatures != null) {
      cloneEdgeFeatures = new Feature[edgeFeatures.length];
      System.arraycopy(edgeFeatures, 0, cloneEdgeFeatures, 0, edgeFeatures.length);
    }

    Map<String, float[]> cloneWeights = null;
    float[] cWeight = null;
    if (weights != null) {
      cloneWeights = new HashMap();
      for (Map.Entry<String, float[]> weight : weights.entrySet()) {
        cWeight = new float[weight.getValue().length];
        System.arraycopy(weight.getValue(), 0, cWeight, 0, weight.getValue().length);
        cloneWeights.put(weight.getKey(), cWeight);
      }
    }

    float[] cloneLables = null;
    if (labels != null) {
      cloneLables = new float[labels.length];
      System.arraycopy(labels, 0, cloneLables, 0, labels.length);
    }

    Map<String, AliasTable> cloneAliasTables = null;
    AliasTable cAliasTable = null;
    if (aliasTables != null) {
      cloneAliasTables = new HashMap();
      for (Map.Entry<String, AliasTable> alias : aliasTables.entrySet()) {
        cAliasTable = (AliasTable) aliasTables.get(alias.getKey()).deepClone();
        cloneAliasTables.put(alias.getKey(), cAliasTable);
      }
    }

    MultiGraphNode node = new MultiGraphNode(cloneFeats, cloneNeighbors, cloneTypes, cloneEdgeTypes,
            cloneEdgeFeatures, cloneWeights, cloneLables);
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
      ByteBufSerdeUtils.serializeInt(output, neighbors.size());
      for (Map.Entry<String, Neighbor> pair: neighbors.entrySet()) {
        ByteBufSerdeUtils.serializeBytes(output, pair.getKey().getBytes());
        ByteBufSerdeUtils.serializeByte(output, pair.getValue().getStorageType().getIndex());
        pair.getValue().serialize(output);
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }

    if (types != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeIntsMap(output, types);
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
      ByteBufSerdeUtils.serializeFloatsMap(output, weights);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (labels != null) {
      ByteBufSerdeUtils.serializeBoolean(output, true);
      ByteBufSerdeUtils.serializeFloats(output, labels);
    } else {
      ByteBufSerdeUtils.serializeBoolean(output, false);
    }

    if (aliasTables != null) {
      ByteBufSerdeUtils.serializeInt(output, aliasTables.size());
      for (Map.Entry<String, AliasTable> pair: aliasTables.entrySet()) {
        ByteBufSerdeUtils.serializeBytes(output, pair.getKey().getBytes());
        pair.getValue().serialize(output);
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
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

    int neighborsSize = ByteBufSerdeUtils.deserializeInt(input);
    if (neighborsSize > 0) {
      neighbors = new HashMap<>(neighborsSize);
      for (int i = 0; i < neighborsSize; i++) {
        String key = new String(ByteBufSerdeUtils.deserializeBytes(input));
        NeighborStorageType storageType = NeighborStorageType.valueOf(ByteBufSerdeUtils.deserializeByte(input));
        Neighbor neighbor;
        switch (storageType) {
          case LONGARRAY: neighbor = new LongNeighbor(); break;
          case INTARRAY: neighbor = new IntNeighbor(); break;
          default: neighbor = new LongNeighbor(); break;
        }
        neighbor.deserialize(input);
        neighbors.put(key, neighbor);
      }
    }

    boolean typesFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (typesFlag) {
      types = ByteBufSerdeUtils.deserializeIntsMap(input);
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
      weights = ByteBufSerdeUtils.deserializeFloatsMap(input);
    }

    boolean labelsFlag = ByteBufSerdeUtils.deserializeBoolean(input);
    if (labelsFlag) {
      labels = ByteBufSerdeUtils.deserializeFloats(input);
    }

    int aliasTableSize = ByteBufSerdeUtils.deserializeInt(input);
    if (aliasTableSize > 0) {
      for (int i = 0; i < aliasTableSize; i++) {
        String key = new String(ByteBufSerdeUtils.deserializeBytes(input));
        AliasTable aliasTable = (AliasTable)ByteBufSerdeUtils.deserializeObject(input);
        aliasTables.put(key, aliasTable);
      }
    }
  }

  @Override
  public int bufferLen() {
    // flags len for init value: 8*4
    int len = 5 * ByteBufSerdeUtils.BOOLEN_LENGTH + 3 * ByteBufSerdeUtils.INT_LENGTH;
    if (feats != null) {
      len += feats.bufferLen() + ByteBufSerdeUtils.BYTE_LENGTH;
    }

    if (neighbors != null) {
      for (Map.Entry<String, Neighbor> pair: neighbors.entrySet()) {
        len += ByteBufSerdeUtils.serializedBytesLen(pair.getKey().getBytes());
        len += ByteBufSerdeUtils.BYTE_LENGTH;
        len += pair.getValue().bufferLen();
      }
    }

    if (types != null) {
      len += ByteBufSerdeUtils.serializedIntsMapLen(types);
    }

    if (edgeTypes != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(edgeTypes);
    }

    if (edgeFeatures != null) {
      for (Feature edgeFeature : edgeFeatures) {
        len += edgeFeature.bufferLen();
      }
    }

    if (weights != null) {
      len += ByteBufSerdeUtils.serializedFloatsMapLen(weights);
    }

    if (labels != null) {
      len += ByteBufSerdeUtils.serializedFloatsLen(labels);
    }

    if (aliasTables != null) {
      for (Map.Entry<String, AliasTable> pair: aliasTables.entrySet()) {
        len += ByteBufSerdeUtils.serializedBytesLen(pair.getKey().getBytes());
        len += pair.getValue().bufferLen();
      }
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
      StreamSerdeUtils.serializeInt(output, neighbors.size());
      for (Map.Entry<String, Neighbor> pair: neighbors.entrySet()) {
        StreamSerdeUtils.serializeBytes(output, pair.getKey().getBytes());
        StreamSerdeUtils.serializeByte(output, pair.getValue().getStorageType().getIndex());
        pair.getValue().serialize(output);
      }
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }

    if (types != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeIntsMap(output, types);
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
      StreamSerdeUtils.serializeFloatsMap(output, weights);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (labels != null) {
      StreamSerdeUtils.serializeBoolean(output, true);
      StreamSerdeUtils.serializeFloats(output, labels);
    } else {
      StreamSerdeUtils.serializeBoolean(output, false);
    }

    if (aliasTables != null) {
      StreamSerdeUtils.serializeInt(output, aliasTables.size());
      for (Map.Entry<String, AliasTable> pair: aliasTables.entrySet()) {
        StreamSerdeUtils.serializeBytes(output, pair.getKey().getBytes());
        pair.getValue().serialize(output);
      }
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
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

    int neighborsSize = StreamSerdeUtils.deserializeInt(input);
    if (neighborsSize > 0) {
      neighbors = new HashMap<>(neighborsSize);
      for (int i = 0; i < neighborsSize; i++) {
        String key = new String(StreamSerdeUtils.deserializeBytes(input));
        NeighborStorageType storageType = NeighborStorageType.valueOf(StreamSerdeUtils.deserializeByte(input));
        Neighbor neighbor;
        switch (storageType) {
          case LONGARRAY: neighbor = new LongNeighbor(); break;
          case INTARRAY: neighbor = new IntNeighbor(); break;
          default: neighbor = new LongNeighbor(); break;
        }
        neighbor.deserialize(input);
        neighbors.put(key, neighbor);
      }
    }

    boolean typesFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (typesFlag) {
      types = StreamSerdeUtils.deserializeIntsMap(input);
    }

    boolean edgeTypesFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (edgeTypesFlag) {
      edgeTypes = StreamSerdeUtils.deserializeInts(input);
    }

    int edgeFeaturesLen = StreamSerdeUtils.deserializeInt(input);
    if (edgeFeaturesLen > 0) {
      edgeFeatures = new Feature[edgeFeaturesLen];
      for (int i = 0; i < edgeFeaturesLen; i++) {
        edgeFeatures[i] = new Feature();
        edgeFeatures[i].deserialize(input);
      }
    }

    boolean weightsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (weightsFlag) {
      weights = StreamSerdeUtils.deserializeFloatsMap(input);
    }

    boolean labelsFlag = StreamSerdeUtils.deserializeBoolean(input);
    if (labelsFlag) {
      labels = StreamSerdeUtils.deserializeFloats(input);
    }

    int aliasTableSize = StreamSerdeUtils.deserializeInt(input);
    if (aliasTableSize > 0) {
      for (int i = 0; i < aliasTableSize; i++) {
        String key = new String(StreamSerdeUtils.deserializeBytes(input));
        AliasTable aliasTable = (AliasTable)StreamSerdeUtils.deserializeObject(input);
        aliasTables.put(key, aliasTable);
      }
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

}
