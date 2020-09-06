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

package com.tencent.angel.graph.psf.neighbors.samplebyaliastable.initaliastable;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.psf.neighbors.samplebyaliastable.samplealiastable.NeighborsAliasTableElement;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;

/**
 * part node's data in ps
 */
public class PartInitNeighborAliasTableParam extends PartitionUpdateParam {
    private Long2ObjectMap<NeighborsAliasTableElement> nodeId2Neighbors;

    private long[] nodeIds;
    private transient int startIndex;
    private transient int endIndex;

    public PartInitNeighborAliasTableParam(int matrixId, PartitionKey partitionKey,
                                           Long2ObjectMap<NeighborsAliasTableElement> nodeId2Neighbors, long[] nodeIds, int startIndex, int endIndex) {

        super(matrixId, partitionKey);
        this.nodeId2Neighbors = nodeId2Neighbors;
        this.nodeIds = nodeIds;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartInitNeighborAliasTableParam() {
        this(0, null, null, null, 0, 0);
    }

    private void clear() {
        nodeId2Neighbors = null;
        nodeIds = null;
        startIndex = -1;
        endIndex = -1;
    }

    public Long2ObjectMap<NeighborsAliasTableElement> getNodeId2Neighbors() {
        return nodeId2Neighbors;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);

        long nodeId;
        NeighborsAliasTableElement neighbors;
        int writeIndex = buf.writerIndex();
        int numNodesWriten = 0;

        buf.writeInt(0);

        for (int i = startIndex; i < endIndex; i++) {
            // get nodeId
            nodeId = nodeIds[i];
            //get  data the nodeId mapped 1.neighbors 2.tags 3.attrs
            neighbors = nodeId2Neighbors.get(nodeId);
            if (neighbors == null)
                continue;
            buf.writeLong(nodeId);
            buf.writeInt(neighbors.getNodesNum());

            // write out edges
            long[] nbrs = neighbors.getNeighborIds();
            for (int j = 0; j < nbrs.length; j++) {
                buf.writeLong(nbrs[j]);
            }

            // write out tags
            float[] accept = neighbors.getAccept();
            assert (accept.length == nbrs.length);
            for (int j = 0; j < accept.length; j++) {
                buf.writeFloat(accept[j]);
            }

            //write out attrs
            int[] alias = neighbors.getAlias();
            assert (alias.length == nbrs.length);
            for (int j = 0; j < alias.length; j++) {
                buf.writeInt(alias[j]);
            }
            numNodesWriten++;
        }

        buf.setInt(writeIndex, numNodesWriten);
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);

        int len = buf.readInt();
        nodeId2Neighbors = new Long2ObjectArrayMap<>(len);

        for (int i = 0; i < len; i++) {
            long nodeId = buf.readLong();
            int numNeighbor = buf.readInt();
            long[] nbrs = new long[numNeighbor];
            float[] accept = new float[numNeighbor];
            int[] alias = new int[numNeighbor];

            for (int j = 0; j < numNeighbor; j++) {
                nbrs[j] = buf.readLong();
            }
            for (int j = 0; j < numNeighbor; j++) {
                accept[j] = buf.readFloat();
            }
            for (int j = 0; j < numNeighbor; j++) {
                alias[j] = buf.readInt();
            }

            NeighborsAliasTableElement nbrElem = new NeighborsAliasTableElement(nbrs, accept, alias);
            nodeId2Neighbors.put(nodeId, nbrElem);
        }
    }

    @Override
    public int bufferLen() {
        int len = super.bufferLen();
        len += 4;
        for (int i = startIndex; i < endIndex; i++) {
            NeighborsAliasTableElement nbrElem = nodeId2Neighbors.get(nodeIds[i]);
            if (nbrElem != null) {
                int numNodes = nbrElem.getNodesNum();
                len += 12;//nodeId and numNodes
                len += Long.BYTES * numNodes + Float.BYTES * numNodes + Integer.BYTES * numNodes;
            }
        }

        return len;
    }
}
