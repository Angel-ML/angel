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

package com.tencent.angel.spark.ml.psf.triangle;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import io.netty.buffer.ByteBuf;

/**
 * Result of GetNeighborWithByteAttr
 */
public class PartGetNeighborWithAttrResult extends PartitionGetResult {

    private int partId;
    private NeighborsAttrsCompressedElement[] neighborsCompressed;


    public PartGetNeighborWithAttrResult(int partitionId, NeighborsAttrsCompressedElement[] neighborsCompressed) {
        this.partId = partitionId;
        this.neighborsCompressed = neighborsCompressed;
        assert (neighborsCompressed != null);
    }

    public PartGetNeighborWithAttrResult() {
        this(-1, null);
    }

    public NeighborsAttrsCompressedElement[] getNeighborsCompressed() {
        return neighborsCompressed;
    }

    public int getPartId() {
        return partId;
    }

    @Override
    public void serialize(ByteBuf output) {
        output.writeInt(partId);
        output.writeInt(neighborsCompressed.length);
        for (int i = 0; i < neighborsCompressed.length; i++) {
            if (neighborsCompressed[i] == null) {
                output.writeInt(0); // neighborLen
                output.writeInt(0); // attrLen
            } else {
                neighborsCompressed[i].serialize(output);
            }
        }
    }

    @Override
    public void deserialize(ByteBuf input) {
        partId = input.readInt();
        int len = input.readInt();
        neighborsCompressed = new NeighborsAttrsCompressedElement[len];
        for (int i = 0; i < len; i++) {
            NeighborsAttrsCompressedElement elem = new NeighborsAttrsCompressedElement();
            elem.deserialize(input);
            neighborsCompressed[i] = elem;
        }
    }

    @Override
    public int bufferLen() {
        int len = 8;
        for (int i = 0; i < neighborsCompressed.length; i++) {
            if (neighborsCompressed[i] == null) {
                len += 4 * 2;
            } else {
                len += neighborsCompressed[i].bufferLen();
            }
        }
        return len;
    }
}
