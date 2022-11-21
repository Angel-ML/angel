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


package com.tencent.angel.ps.storage.vector.element;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.common.StreamSerdeUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;


public class LongFloatArrayPairElement implements IElement {

    private long[] data;
    private float[] weights;

    public LongFloatArrayPairElement(long[] data, float[] weights) {
        this.data = data;
        this.weights = weights;
    }

    public LongFloatArrayPairElement() {
        this(null, null);
    }

    public long[] getData() { return data; }

    public float[] getWeights() { return weights; }

    public void setData(long[] data) {
        this.data = data;
    }

    @Override
    public LongFloatArrayPairElement deepClone() {
        long[] newData = new long[data.length];
        float[] newWeights = new float[weights.length];
        System.arraycopy(data, 0, newData, 0, data.length);
        System.arraycopy(weights, 0, newWeights, 0, weights.length);
        return new LongFloatArrayPairElement(newData, newWeights);
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeLongs(output, data);
        ByteBufSerdeUtils.serializeFloats(output, weights);
    }

    @Override
    public void deserialize(ByteBuf input) {
        data = ByteBufSerdeUtils.deserializeLongs(input);
        weights = ByteBufSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int bufferLen() {
        return ByteBufSerdeUtils.serializedLongsLen(data) + ByteBufSerdeUtils.serializedFloatsLen(weights);
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        StreamSerdeUtils.serializeLongs(output, data);
        StreamSerdeUtils.serializeFloats(output, weights);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        data = StreamSerdeUtils.deserializeLongs(input);
        weights = StreamSerdeUtils.deserializeFloats(input);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }
}
