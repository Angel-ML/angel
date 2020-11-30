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

package com.tencent.angel.graph.client.getNodeAttrs;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetNeighbor
 */
public class PartGetNodeAttrsResult extends PartitionGetResult {

    private int partId;
    /**
     * Node id to neighbors map
     */
    private float[][] nodeIdToAttrs;

    public PartGetNodeAttrsResult(int partId, float[][] nodeIdToAttrs) {
        this.partId = partId;
        this.nodeIdToAttrs = nodeIdToAttrs;
    }

    public PartGetNodeAttrsResult() {
        this(-1, null);
    }

    public float[][] getNodeIdToAttrs() {
        return nodeIdToAttrs;
    }

    public void setNodeIdToAttrs(
            float[][] nodeIdToAttrs) {
        this.nodeIdToAttrs = nodeIdToAttrs;
    }

    public int getPartId() {
        return partId;
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(partId);
        output.writeInt(nodeIdToAttrs.length);
        for (int i = 0; i < nodeIdToAttrs.length; i++) {
            if (nodeIdToAttrs[i] == null) {
                output.writeInt(0);
            } else {
                output.writeInt(nodeIdToAttrs[i].length);
                for (float value : nodeIdToAttrs[i]) {
                    output.writeFloat(value);
                }
            }
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        partId = input.readInt();
        int size = input.readInt();
        nodeIdToAttrs = new float[size][];
        for (int i = 0; i < size; i++) {
            float[] attrs = new float[input.readInt()];
            for (int j = 0; j < attrs.length; j++) {
                attrs[j] = input.readFloat();
            }
            nodeIdToAttrs[i] = attrs;
        }
    }

    @Override
    public int bufferLen() {
        int len = 8;
        for (int i = 0; i < nodeIdToAttrs.length; i++) {
            if (nodeIdToAttrs[i] == null) {
                len += 4;
            } else {
                len += 4;
                len += 8 * nodeIdToAttrs[i].length;
            }
        }
        return len;
    }
}
