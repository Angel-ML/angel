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
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

/**
 * Graph Node Base Data Struct: node contains features, neighbors, node types, edge types, edge
 * features, edge weights and node labels.
 */
public class GraphNode implements IElement {

    private IntFloatVector feats; // node's feature
    private long[] neighbors; // node's neighbors
    private int[] types; // types of neighbors (include the node itself)
    private int[] edgeTypes; // types of edges
    private IntFloatVector[] edgeFeatures; // edges' features
    private float[] weights; // edges' weights
    private float[] labels; // node or edges' labels, for multi-label node classification or edge classification separately

    public GraphNode(IntFloatVector feats, long[] neighbors, int[] types, int[] edgeTypes,
                     IntFloatVector[] edgeFeatures, float[] weights, float[] labels) {
        this.feats = feats;
        this.neighbors = neighbors;
        this.types = types;
        this.edgeTypes = edgeTypes;
        this.edgeFeatures = edgeFeatures;
        this.weights = weights;
        this.labels = labels;
    }

    public GraphNode(IntFloatVector feats, long[] neighbors) {
        this(feats, neighbors, null, null,
                null, null, null);
    }

    public GraphNode() {
        this(null, null, null, null,
                null, null, null);
    }

    public IntFloatVector getFeats() {
        return feats;
    }

    public void setFeats(IntFloatVector feats) {
        this.feats = feats;
    }

    public long[] getNeighbors() {
        return neighbors;
    }

    public void setNeighbors(long[] neighbors) {
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

    public IntFloatVector[] getEdgeFeatures() {
        return edgeFeatures;
    }

    public void setEdgeFeatures(IntFloatVector[] edgeFeatures) {
        this.edgeFeatures = edgeFeatures;
    }

    public float[] getWeights() {
        return weights;
    }

    public void setWeights(float[] weights) {
        this.weights = weights;
    }

    public float[] getLabels() {
        return labels;
    }

    public void setLabels(float[] labels) {
        this.labels = labels;
    }

    @Override
    public GraphNode deepClone() {
        IntFloatVector cloneFeats = null;
        if (feats != null) {
            cloneFeats = feats.clone();
        }

        long[] cloneNeighbors = null;
        if (neighbors != null) {
            cloneNeighbors = new long[neighbors.length];
            System.arraycopy(neighbors, 0, cloneNeighbors, 0, neighbors.length);
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

        IntFloatVector[] cloneEdgeFeatures = null;
        if (edgeFeatures != null) {
            cloneEdgeFeatures = new IntFloatVector[edgeFeatures.length];
            System.arraycopy(edgeFeatures, 0, cloneEdgeFeatures, 0, edgeFeatures.length);
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

        return new GraphNode(cloneFeats, cloneNeighbors, cloneTypes, cloneEdgeTypes,
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
            ByteBufSerdeUtils.serializeLongs(output, neighbors);
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }
        if (types != null) {
            ByteBufSerdeUtils.serializeInt(output, 1);
            ByteBufSerdeUtils.serializeInts(output, types);
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
            ByteBufSerdeUtils.serializeFloats(output, weights);
        } else {
            ByteBufSerdeUtils.serializeInt(output, 0);
        }
        if (labels != null) {
            ByteBufSerdeUtils.serializeInt(output, 1);
            ByteBufSerdeUtils.serializeFloats(output, labels);
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
            neighbors = ByteBufSerdeUtils.deserializeLongs(input);
        }
        int typesFlag = ByteBufSerdeUtils.deserializeInt(input);
        if (typesFlag > 0) {
            types = ByteBufSerdeUtils.deserializeInts(input);
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
            weights = ByteBufSerdeUtils.deserializeFloats(input);
        }
        int labelsFlag = ByteBufSerdeUtils.deserializeInt(input);
        if (labelsFlag > 0) {
            weights = ByteBufSerdeUtils.deserializeFloats(input);
        }
    }

    @Override
    public int bufferLen() {
        // flags len for init value: 7*4
        int len = 7 * ByteBufSerdeUtils.INT_LENGTH;
        if (feats != null) {
            len += ByteBufSerdeUtils.serializedVectorLen(feats);
        }
        if (neighbors != null) {
            len += ByteBufSerdeUtils.serializedLongsLen(neighbors);
        }
        if (types != null) {
            len += ByteBufSerdeUtils.serializedIntsLen(types);
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
            len += ByteBufSerdeUtils.serializedFloatsLen(weights);
        }
        if (labels != null) {
            len += ByteBufSerdeUtils.serializedFloatsLen(labels);
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
            StreamSerdeUtils.serializeLongs(output, neighbors);
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }
        if (types != null) {
            StreamSerdeUtils.serializeInt(output, 1);
            StreamSerdeUtils.serializeInts(output, types);
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
            StreamSerdeUtils.serializeFloats(output, weights);
        } else {
            StreamSerdeUtils.serializeInt(output, 0);
        }
        if (labels != null) {
            StreamSerdeUtils.serializeInt(output, 1);
            StreamSerdeUtils.serializeFloats(output, labels);
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
            neighbors = StreamSerdeUtils.deserializeLongs(input);
        }
        int typesFlag = StreamSerdeUtils.deserializeInt(input);
        if (typesFlag > 0) {
            types = StreamSerdeUtils.deserializeInts(input);
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
            weights = StreamSerdeUtils.deserializeFloats(input);
        }
        int labelsFlag = StreamSerdeUtils.deserializeInt(input);
        if (labelsFlag > 0) {
            weights = StreamSerdeUtils.deserializeFloats(input);
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}