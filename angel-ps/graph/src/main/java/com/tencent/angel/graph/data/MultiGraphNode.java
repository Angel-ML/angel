package com.tencent.angel.graph.data;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MultiGraphNode implements IElement {

  private IntFloatVector feats; // node's feature
  private Map<String, long[]> neighbors; // node's neighbors
  private Map<String, int[]> types; // types of neighbors (include the node itself)
  private int[] edgeTypes; // types of edges
  private IntFloatVector[] edgeFeatures; // edges' features
  private Map<String, float[]> weights; // edges' weights
  private float[] labels; // node or edges' labels, for multi-label node classification or edge classification separately
  private Map<String, AliasTable> aliasTables;// alias table for weighted sampling

  public MultiGraphNode(IntFloatVector feats, Map<String, long[]> neighbors, Map<String, int[]> types, int[] edgeTypes,
                        IntFloatVector[] edgeFeatures, Map<String, float[]> weights, float[] labels) {
    this.feats = feats;
    this.neighbors = neighbors;
    this.types = types;
    this.edgeTypes = edgeTypes;
    this.edgeFeatures = edgeFeatures;
    this.weights = weights;
    this.labels = labels;
  }

  public MultiGraphNode(IntFloatVector feats, Map<String, long[]> neighbors, Map<String, int[]> types, int[] edgeTypes,
                        IntFloatVector[] edgeFeatures, Map<String, float[]> weights, float[] labels, Map<String, AliasTable> aliasTables) {
    this(feats, neighbors, types, edgeTypes, edgeFeatures, weights, labels);
    this.aliasTables = aliasTables;
  }

  public MultiGraphNode(IntFloatVector feats, Map<String, long[]> neighbors) {
    this(feats, neighbors, null, null,
            null, null, null);
  }

  public MultiGraphNode() {
    this(null, null, null, null,
            null, null, null);
  }

  public IntFloatVector getFeats() {
    return feats;
  }

  public void setFeats(IntFloatVector feats) {
    this.feats = feats;
  }

  public Map<String, long[]> getNeighbors() {
    return neighbors;
  }

  public long[] getNeighbors(String name) {
    return neighbors.get(name);
  }

  public void setNeighbors(Map<String, long[]> neighbors) {
    this.neighbors = neighbors;
  }

  public void setNeighbors(String name, long[] neighs) {
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

  public IntFloatVector[] getEdgeFeatures() {
    return edgeFeatures;
  }

  public void setEdgeFeatures(IntFloatVector[] edgeFeatures) {
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
    IntFloatVector cloneFeats = null;
    if (feats != null) {
      cloneFeats = feats.clone();
    }

    Map<String, long[]> cloneNeighbors = null;
    long[] cNeigh = null;
    if (neighbors != null) {
      cloneNeighbors = new HashMap();
      for (Map.Entry<String, long[]> neigh : neighbors.entrySet()) {
        cNeigh = new long[neigh.getValue().length];
        System.arraycopy(neigh.getValue(), 0, cNeigh, 0, neigh.getValue().length);
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

    IntFloatVector[] cloneEdgeFeatures = null;
    if (edgeFeatures != null) {
      cloneEdgeFeatures = new IntFloatVector[edgeFeatures.length];
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

    return new MultiGraphNode(cloneFeats, cloneNeighbors, cloneTypes, cloneEdgeTypes,
            cloneEdgeFeatures, cloneWeights, cloneLables);
  }

  @Override
  public void serialize(ByteBuf output) {
    if (feats != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeVector(output, feats);
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
    if (neighbors != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeLongsMap(output, neighbors);
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
    if (types != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeIntsMap(output, types);
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
    if (edgeTypes != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeInts(output, edgeTypes);
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
    if (edgeFeatures != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeInt(output, edgeFeatures.length);
      for (IntFloatVector edgeFeature : edgeFeatures) {
        ByteBufSerdeUtils.serializeVector(output, edgeFeature);
      }
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
    if (weights != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeFloatsMap(output, weights);
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
    }
    if (labels != null) {
      ByteBufSerdeUtils.serializeInt(output, 1);
      ByteBufSerdeUtils.serializeFloats(output, labels);
    } else {
      ByteBufSerdeUtils.serializeInt(output, 0);
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
    int featsFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (featsFlag > 0) {
      feats = (IntFloatVector) ByteBufSerdeUtils.deserializeVector(input);
    }
    int neighborsFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (neighborsFlag > 0) {
      neighbors = ByteBufSerdeUtils.deserializeLongsMap(input);
    }
    int typesFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (typesFlag > 0) {
      types = ByteBufSerdeUtils.deserializeIntsMap(input);
    }
    int edgeTypesFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (edgeTypesFlag > 0) {
      edgeTypes = ByteBufSerdeUtils.deserializeInts(input);
    }
    int edgeFeaturesFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (edgeFeaturesFlag > 0) {
      int len = ByteBufSerdeUtils.deserializeInt(input);
      edgeFeatures = new IntFloatVector[len];
      for (int i = 0; i < len; i++) {
        edgeFeatures[i] = (IntFloatVector) ByteBufSerdeUtils.deserializeVector(input);
      }
    }
    int weightsFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (weightsFlag > 0) {
      weights = ByteBufSerdeUtils.deserializeFloatsMap(input);
    }
    int labelsFlag = ByteBufSerdeUtils.deserializeInt(input);
    if (labelsFlag > 0) {
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
    int len = 8 * ByteBufSerdeUtils.INT_LENGTH;
    if (feats != null) {
      len += ByteBufSerdeUtils.serializedVectorLen(feats);
    }
    if (neighbors != null) {
      len += ByteBufSerdeUtils.serializedLongsMapLen(neighbors);
    }
    if (types != null) {
      len += ByteBufSerdeUtils.serializedIntsMapLen(types);
    }
    if (edgeTypes != null) {
      len += ByteBufSerdeUtils.serializedIntsLen(edgeTypes);
    }
    if (edgeFeatures != null) {
      len += ByteBufSerdeUtils.INT_LENGTH;
      for (IntFloatVector edgeFeature : edgeFeatures) {
        len += ByteBufSerdeUtils.serializedVectorLen(edgeFeature);
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
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeVector(output, feats);
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }
    if (neighbors != null) {
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeLongsMap(output, neighbors);
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }
    if (types != null) {
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeIntsMap(output, types);
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }
    if (edgeTypes != null) {
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeInts(output, edgeTypes);
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }
    if (edgeFeatures != null) {
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeInt(output, edgeFeatures.length);
      for (IntFloatVector edgeFeature : edgeFeatures) {
        StreamSerdeUtils.serializeVector(output, edgeFeature);
      }
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }
    if (weights != null) {
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeFloatsMap(output, weights);
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
    }
    if (labels != null) {
      StreamSerdeUtils.serializeInt(output, 1);
      StreamSerdeUtils.serializeFloats(output, labels);
    } else {
      StreamSerdeUtils.serializeInt(output, 0);
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
    int featsFlag = StreamSerdeUtils.deserializeInt(input);
    if (featsFlag > 0) {
      feats = (IntFloatVector) StreamSerdeUtils.deserializeVector(input);
    }
    int neighborsFlag = StreamSerdeUtils.deserializeInt(input);
    if (neighborsFlag > 0) {
      neighbors = StreamSerdeUtils.deserializeLongsMap(input);
    }
    int typesFlag = StreamSerdeUtils.deserializeInt(input);
    if (typesFlag > 0) {
      types = StreamSerdeUtils.deserializeIntsMap(input);
    }
    int edgeTypesFlag = StreamSerdeUtils.deserializeInt(input);
    if (edgeTypesFlag > 0) {
      edgeTypes = StreamSerdeUtils.deserializeInts(input);
    }
    int edgeFeaturesFlag = StreamSerdeUtils.deserializeInt(input);
    if (edgeFeaturesFlag > 0) {
      int len = StreamSerdeUtils.deserializeInt(input);
      edgeFeatures = new IntFloatVector[len];
      for (int i = 0; i < len; i++) {
        edgeFeatures[i] = (IntFloatVector) StreamSerdeUtils.deserializeVector(input);
      }
    }
    int weightsFlag = StreamSerdeUtils.deserializeInt(input);
    if (weightsFlag > 0) {
      weights = StreamSerdeUtils.deserializeFloatsMap(input);
    }
    int labelsFlag = StreamSerdeUtils.deserializeInt(input);
    if (labelsFlag > 0) {
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
