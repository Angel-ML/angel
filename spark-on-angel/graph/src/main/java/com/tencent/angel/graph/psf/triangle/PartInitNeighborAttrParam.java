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
package com.tencent.angel.graph.psf.triangle;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

public class PartInitNeighborAttrParam extends PartitionUpdateParam {

    private Long2ObjectMap<NeighborsAttrsElement> nodeId2Neighbors;

    private long[] nodeIds;
    private transient int startIndex;
    private transient int endIndex;

    public PartInitNeighborAttrParam(int matrixId, PartitionKey partitionKey,
            Long2ObjectMap<NeighborsAttrsElement> nodeId2Neighbors, long[] nodeIds, int startIndex, int endIndex) {

        super(matrixId, partitionKey);
        this.nodeId2Neighbors = nodeId2Neighbors;
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartInitNeighborAttrParam() {
        this(0, null, null, null, 0, 0);
    }


    private void clear() {
        nodeId2Neighbors = null;
        nodeIds = null;
        startIndex = -1;
        endIndex = -1;
    }

    public Long2ObjectMap<NeighborsAttrsElement> getNodeId2Neighbors() {
        return nodeId2Neighbors;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);

        long nodeId;
        NeighborsAttrsElement neighbors;
        int writeIndex = buf.writerIndex();
        int numNodesWritten = 0;

        buf.writeInt(0);
        for (int i = startIndex; i < endIndex; i++) {
            nodeId = nodeIds[i];
            neighbors = nodeId2Neighbors.get(nodeId);
            if (neighbors == null)
                continue;

            buf.writeLong(nodeId);
            buf.writeInt(neighbors.getNumNodes());

            // write out edges
            long[] edges = neighbors.getNeighborIds();
            for (int j = 0; j < edges.length; j++) {
                buf.writeLong(edges[j]);
            }

            byte[] attrs = neighbors.getAttrs();
            assert (attrs.length == neighbors.getNumNodes());
            // write out tags
            for (int j = 0; j < attrs.length; j++) {
                buf.writeByte(attrs[j]);
            }

            numNodesWritten++;
        }

        buf.setInt(writeIndex, numNodesWritten);

        clear();
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);

        int len = buf.readInt();
        nodeId2Neighbors = new Long2ObjectArrayMap<>(len);

        for (int i = 0; i < len; i++) {
            long nodeId = buf.readLong();
            int numNeighbor = buf.readInt();
            long[] edges = new long[numNeighbor];
            byte[] tags = new byte[numNeighbor];

            for (int j = 0; j < numNeighbor; j++) {
                edges[j] = buf.readLong();
            }
            for (int j = 0; j < numNeighbor; j++) {
                tags[j] = buf.readByte();
            }

            NeighborsAttrsElement tcElem = new NeighborsAttrsElement(edges, tags);

            nodeId2Neighbors.put(nodeId, tcElem);
        }
    }


    @Override
    public int bufferLen() {
        int len = super.bufferLen();
        len += 4;
        for (int i = startIndex; i < endIndex; i++) {
            NeighborsAttrsElement tcElem = nodeId2Neighbors.get(nodeIds[i]);
            if (tcElem != null) {
                int numNodes = tcElem.getNumNodes();
                len += 12;  // nodeId and numNodes
                len += Long.BYTES * numNodes + Byte.BYTES * numNodes;
            }
        }

        return len;
    }


}
