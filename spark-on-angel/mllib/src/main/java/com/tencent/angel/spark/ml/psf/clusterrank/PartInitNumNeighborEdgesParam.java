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
package com.tencent.angel.spark.ml.psf.clusterrank;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;

public class PartInitNumNeighborEdgesParam extends PartitionUpdateParam {

    private Long2LongMap nodeIdToNumEdges;

    private long[] nodeIds;
    private transient int startIndex;
    private transient int endIndex;

    public PartInitNumNeighborEdgesParam(int matrixId, PartitionKey partKey,
            Long2LongMap nodeIdToNumEdges, long[] nodeIds, int startIndex, int endIndex) {
        super(matrixId, partKey);
        this.nodeIdToNumEdges = nodeIdToNumEdges;
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartInitNumNeighborEdgesParam() {
        this(0, null, null, null, 0, 0);
    }

    public Long2LongMap getNodeIdToNumEdges() {
        return nodeIdToNumEdges;
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    private void clear() {
        nodeIdToNumEdges = null;
        nodeIds = null;
        startIndex = -1;
        endIndex = -1;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);
        long nodeId;
        long numEdges;
        int writeIndex = buf.writerIndex();
        int writeNum = 0;
        buf.writeInt(0);
        for (int i = startIndex; i < endIndex; i++) {
            nodeId = nodeIds[i];
            numEdges = nodeIdToNumEdges.get(nodeId);
            buf.writeLong(nodeId);
            buf.writeLong(numEdges);
            writeNum++;
        }
        buf.setInt(writeIndex, writeNum);

        clear();
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);
        int len = buf.readInt();
        nodeIdToNumEdges = new Long2LongOpenHashMap(len);

        for (int i = 0; i < len; i++) {
            long nodeId = buf.readLong();
            long numEdges = buf.readLong();
            nodeIdToNumEdges.put(nodeId, numEdges);
        }
    }

    @Override
    public int bufferLen() {
        int len = super.bufferLen();
        len += 4;
        int numItem = nodeIdToNumEdges.size();
        len += 8 * numItem; // key
        len += 8 * numItem; // value
        return len;
    }


}
