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


package com.tencent.angel.graph.model.neighbor.dynamicsimple;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import com.tencent.angel.ps.storage.vector.element.IElement;
import io.netty.buffer.ByteBuf;
import org.apache.commons.lang.ArrayUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Random;


public class DynamicSimpleNeighborElement implements IElement {

    private long[] data;
    private float[] weights;
    private int[] alias;
    private int index;
    private int[] types;
    private int[] indptr; // indicate the index of nbrs' type
    private int[] indexes;


    public DynamicSimpleNeighborElement(long[] data, int index) {
        this.data = data;
        this.index = index;
    }

    public DynamicSimpleNeighborElement(long[] data, float[] weights, int index) {
        this.data = data;
        this.weights = weights;
        this.index = index;
    }

    public DynamicSimpleNeighborElement(long[] data, float[] weights, int[] alias, int index) {
        this.data = data;
        this.alias = alias;
        this.weights = weights;
        this.index = index;
    }

    public DynamicSimpleNeighborElement(long[] data, int[] types, int[] indptr, int[] indexes) {
        this.data = data;
        this.types = types;
        this.indptr = indptr;
        this.indexes = indexes;
        this.index = -1;
    }

    public DynamicSimpleNeighborElement(long[] data, float[] weights, int[] types, int[] indptr, int[] indexes) {
        this.data = data;
        this.weights = weights;
        this.types = types;
        this.indptr = indptr;
        this.indexes = indexes;
        this.index = -1;
    }

    public DynamicSimpleNeighborElement(long[] data, float[] weights, int[] alias, int[] types, int[] indptr, int[] indexes) {
        this.data = data;
        this.weights = weights;
        this.alias = alias;
        this.types = types;
        this.indptr = indptr;
        this.indexes = indexes;
        this.index = -1;
    }

    public DynamicSimpleNeighborElement() {
        this(null, null, null, -1);
    }

    public long[] getData() {
        return data;
    }

    public void setData(long[] data) {
        this.data = data;
    }

    public void setWeights(float[] weights) {
        if (this.weights == null) {
            this.weights = weights;
        } else {
            System.arraycopy(weights, 0, this.weights, 0, weights.length);
        }
    }

    public float[] getWeights() { return this.weights; }

    public int[] getTypes() { return this.types; }

    public int[] getIndptr() { return this.indptr; }

    public void setAlias(int[] alias) { this.alias = alias; }

    public void add(long[] data_, float[] weights_) {
        System.arraycopy(data_, 0, data, index, data_.length);
        System.arraycopy(weights_, 0, weights, index, weights_.length);
        index += data_.length;
    }

    public void add(long[] data_) {
        System.arraycopy(data_, 0, data, index, data_.length);
        index += data_.length;
    }

    public void add(long[] data_, boolean isAttempt) {
        if (isAttempt) {
            if (ArrayUtils.lastIndexOf(data, data_[0], index-1) == -1) { add(data_); }
        } else { add(data_); }
    }

    public void add(long[] data_, float[] weights_, boolean isAttempt) {
        if (isAttempt) {
            if (ArrayUtils.lastIndexOf(data, data_[0], index-1) == -1) { add(data_, weights_); }
        } else { add(data_, weights_); }
    }

    public void add(long[] data_, int[] types_, int[] indptr_) {
        for(int i = 0; i < types_.length; i++) {
            int typeIdx = ArrayUtils.indexOf(types, types_[i]);

            int min = indptr_[i];
            int max = indptr_[i + 1];
            System.arraycopy(data_, min, data, indexes[typeIdx],max-min);
            indexes[typeIdx] += (max-min);
        }
    }

    public void add(long[] data_, int[] types_, int[] indptr_, boolean isAttempt) {
        if (isAttempt) {
            int typeIdx = ArrayUtils.indexOf(types, types_[0]);
            if (ArrayUtils.lastIndexOf(data, data_[0], indexes[typeIdx]-1) == -1) { add(data_, types_, indptr_); }
        } else { add(data_, types_, indptr_); }
    }

    public void add(long[] data_, int[] types_, int[] indptr_, float[] weights_) {
        for(int i = 0; i < types_.length; i++) {
            int typeIdx = ArrayUtils.indexOf(types, types_[i]);

            int min = indptr_[i];
            int max = indptr_[i + 1];
            System.arraycopy(data_, min, data, indexes[typeIdx],max-min);
            System.arraycopy(weights_, min, weights, indexes[typeIdx],max-min);
            indexes[typeIdx] += (max-min);
        }
    }

    public void add(long[] data_, int[] types_, int[] indptr_, float[] weights_, boolean isAttempt) {
        if (isAttempt) {
            int typeIdx = ArrayUtils.indexOf(types, types_[0]);
            if (ArrayUtils.lastIndexOf(data, data_[0], indexes[typeIdx]-1) == -1) { add(data_, types_, indptr_, weights_); }
        } else { add(data_, types_, indptr_, weights_); }
    }

    public long sample(Random r, long nodeId) {
        if (data == null || index < 1) return nodeId;
        else {
            int idx = r.nextInt(index);
            if (weights == null || weights.length < 1) return data[idx];
            else {
                float accept = r.nextFloat();
                if (accept < weights[idx]) return data[idx];
                else return data[alias[idx]];
            }
        }
    }

    public long[] Sample(boolean sampleByType, int sampleType, Random random, long key, int count) {
        if (weights == null)
            return sample(sampleByType, sampleType, random, key, count);
        else
            return sampleWithAlias(sampleByType, sampleType, random, key, count);
    }

    private long[] sample(boolean sampleByType, int sampleType, Random random, long key, int count) {
        long[] re = new long[count];
        if (types == null || !sampleByType) {
            for (int i = 0; i < count; i ++) {
                re[i] = sample(0, data.length, random);
            }
        } else {
            int typeIdx = ArrayUtils.indexOf(types, sampleType);
            if (typeIdx < 0) {
                Arrays.fill(re, key);
            } else {
                int min = indptr[typeIdx];
                int max = indptr[typeIdx + 1];
                for (int i = 0; i < count; i ++) {
                    re[i] = sample(min, max, random);
                }
            }
        }
        return re;
    }

    private long[] sampleWithAlias(boolean sampleByType, int sampleType, Random random, long key, int count) {
        long[] re = new long[count];
        if (types == null || !sampleByType) {
            for (int i = 0; i < count; i ++) {
                re[i] = sampleWithAlias(0, data.length, random);
            }
        } else {
            int typeIdx = ArrayUtils.indexOf(types, sampleType);
            if (typeIdx < 0) {
                Arrays.fill(re, key);
            } else {
                int min = indptr[typeIdx];
                int max = indptr[typeIdx + 1];
                for (int i = 0; i < count; i ++) {
                    re[i] = sampleWithAlias(min, max, random);
                }
            }
        }
        return re;
    }

    private long sampleWithAlias(int min, int max, Random random) {
        int idx = random.nextInt(max) % (max - min) + min;
        float acc = random.nextFloat();
        if (acc < weights[idx])
            return data[idx];
        else
            return data[alias[idx]+min];
    }

    private long sample(int min, int max, Random random) {
        int idx = random.nextInt(max) % (max - min) + min;
        return data[idx];
    }

    @Override
    public DynamicSimpleNeighborElement deepClone() {
        long[] newData = new long[data.length];
        System.arraycopy(data, 0, newData, 0, data.length);
        if (types == null) {
            if (weights == null) {
                return new DynamicSimpleNeighborElement(newData, newData.length);
            } else {
                float[] newWeights = new float[data.length];
                int[] newAlias = new int[data.length];
                System.arraycopy(weights, 0, newWeights, 0, data.length);
                System.arraycopy(alias, 0, newAlias, 0, data.length);
                return new DynamicSimpleNeighborElement(newData, newWeights, newAlias, newData.length);
            }
        } else {
            if (weights == null) {
                int[] newTypes = new int[types.length];
                int[] newIndptr = new int[indptr.length];
                int[] newIndexes = new int[indexes.length];
                System.arraycopy(types, 0, newTypes, 0, types.length);
                System.arraycopy(indptr, 0, newIndptr, 0, indptr.length);
                System.arraycopy(indexes, 0, newIndexes, 0, indexes.length);
                return new DynamicSimpleNeighborElement(newData, newTypes, newIndptr, newIndexes);
            } else {
                float[] newWeights = new float[data.length];
                int[] newAlias = new int[data.length];
                int[] newTypes = new int[types.length];
                int[] newIndptr = new int[indptr.length];
                int[] newIndexes = new int[indexes.length];
                System.arraycopy(weights, 0, newWeights, 0, data.length);
                System.arraycopy(alias, 0, newAlias, 0, data.length);
                System.arraycopy(types, 0, newTypes, 0, types.length);
                System.arraycopy(indptr, 0, newIndptr, 0, indptr.length);
                System.arraycopy(indexes, 0, newIndexes, 0, indexes.length);
                return new DynamicSimpleNeighborElement(newData, newWeights, newAlias, newTypes, newIndptr, newIndexes);
            }
        }
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeInt(output, index);
        ByteBufSerdeUtils.serializeLongs(output, data);
        if (weights == null) {
            ByteBufSerdeUtils.serializeInt(output, 0);
        } else {
            ByteBufSerdeUtils.serializeInt(output, 1);
            ByteBufSerdeUtils.serializeFloats(output, weights);
            ByteBufSerdeUtils.serializeInts(output, alias);
        }
        if (types == null) {
            ByteBufSerdeUtils.serializeInt(output, 0);
        } else {
            ByteBufSerdeUtils.serializeInt(output, 1);
            ByteBufSerdeUtils.serializeInts(output, types);
            ByteBufSerdeUtils.serializeInts(output, indptr);
            ByteBufSerdeUtils.serializeInts(output, indexes);
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        index = ByteBufSerdeUtils.deserializeInt(input);
        data = ByteBufSerdeUtils.deserializeLongs(input);
        int weightsTag = ByteBufSerdeUtils.deserializeInt(input);
        if (weightsTag == 1) {
            weights = ByteBufSerdeUtils.deserializeFloats(input);
            alias = ByteBufSerdeUtils.deserializeInts(input);
        }
        int typesTag = ByteBufSerdeUtils.deserializeInt(input);
        if (typesTag == 1) {
            types = ByteBufSerdeUtils.deserializeInts(input);
            indptr = ByteBufSerdeUtils.deserializeInts(input);
            indexes = ByteBufSerdeUtils.deserializeInts(input);
        }
    }

    @Override
    public int bufferLen() {
        int len = 0;
        len += ByteBufSerdeUtils.serializedIntLen(index);
        len += ByteBufSerdeUtils.serializedLongsLen(data);
        len += 4;
        if (weights != null) {
            len += ByteBufSerdeUtils.serializedFloatsLen(weights);
            len += ByteBufSerdeUtils.serializedIntsLen(alias);
        }
        len += 4;
        if (types != null) {
            len += ByteBufSerdeUtils.serializedIntsLen(types);
            len += ByteBufSerdeUtils.serializedIntsLen(indptr);
            len += ByteBufSerdeUtils.serializedIntsLen(indexes);
        }
        return len;
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeInt(output, index);
        StreamSerdeUtils.serializeLongs(output, data);
        if (weights == null) {
            StreamSerdeUtils.serializeInt(output, 0);
        } else {
            StreamSerdeUtils.serializeInt(output, 1);
            StreamSerdeUtils.serializeFloats(output, weights);
            StreamSerdeUtils.serializeInts(output, alias);
        }
        if (types == null) {
            StreamSerdeUtils.serializeInt(output, 0);
        } else {
            StreamSerdeUtils.serializeInt(output, 1);
            StreamSerdeUtils.serializeInts(output, types);
            StreamSerdeUtils.serializeInts(output, indptr);
            StreamSerdeUtils.serializeInts(output, indexes);
        }
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        index = StreamSerdeUtils.deserializeInt(input);
        data = StreamSerdeUtils.deserializeLongs(input);
        int weightsTag = StreamSerdeUtils.deserializeInt(input);
        if (weightsTag == 1) {
            weights = StreamSerdeUtils.deserializeFloats(input);
            alias = StreamSerdeUtils.deserializeInts(input);
        }
        int typesTag = StreamSerdeUtils.deserializeInt(input);
        if (typesTag == 1) {
            types = StreamSerdeUtils.deserializeInts(input);
            indptr = StreamSerdeUtils.deserializeInts(input);
            indexes = StreamSerdeUtils.deserializeInts(input);
        }
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
