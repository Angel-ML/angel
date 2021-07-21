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
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class LongEdgeFeats extends LongNeighbor {

    private IntFloatVector[] feats;

    public LongEdgeFeats(long [] nodeIds, IntFloatVector[] feats) {
        super(nodeIds);
        this.feats = feats;
    }

    public LongEdgeFeats() {
        super(null);
    }

    public IntFloatVector[] getFeats() {
        return feats;
    }

    @Override
    public void serialize(ByteBuf output) {
        super.serialize(output);
        ByteBufSerdeUtils.serializeIntFloatVectors(output, feats);
    }

    @Override
    public void deserialize(ByteBuf input) {
        super.deserialize(input);
        feats = ByteBufSerdeUtils.deserializeIntFloatVectors(input);
    }

    @Override
    public int bufferLen() {
        return super.bufferLen() + ByteBufSerdeUtils.serializedIntFloatVectorsLen(feats);
    }

    @Override
    public void serialize(DataOutputStream output) throws IOException {
        super.serialize(output);
        StreamSerdeUtils.serializeIntFloatVectors(output, feats);
    }

    @Override
    public void deserialize(DataInputStream input) throws IOException {
        super.deserialize(input);
        feats = StreamSerdeUtils.deserializeIntFloatVectors(input);
    }

    @Override
    public int dataLen() {
        return bufferLen();
    }

}