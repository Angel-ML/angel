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

package com.tencent.angel.graph.psf.kcore.readTag;

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetNeighbor
 */
public class PartReadTagResult extends PartitionGetResult {
    /**
     * Node id to neighbors map
     */
    private long[] nodes;

    public PartReadTagResult(long[] nodes) {
        this.nodes = nodes;
    }

    public PartReadTagResult() {
        this(null);
    }

    public long[] getNodes() {
        return nodes;
    }

    public void setNodeIdToNeighbors(long[] nodes) {
        this.nodes = nodes;
    }

    @Override
    public void serialize(ByteBuf output) {
        ByteBufSerdeUtils.serializeLongs(output, nodes);
    }

    @Override
    public void deserialize(ByteBuf input) {
        nodes = ByteBufSerdeUtils.deserializeLongs(input);
    }

    @Override
    public int bufferLen() {
        int len = 8;
        len += 8 * nodes.length;
        return len;
    }
}
