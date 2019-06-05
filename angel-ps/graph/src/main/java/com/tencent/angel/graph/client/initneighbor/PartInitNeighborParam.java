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

package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.data.Edge;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import io.netty.buffer.ByteBuf;

import java.util.Map;

public class PartInitNeighborParam extends PartitionUpdateParam {
    private NodeEdgesPair[] nodeEdgesPairs;

    /**
     * Store position: start index in nodeIds
     */
    private int startIndex;

    /**
     * Store position: end index in nodeIds
     */
    private int endIndex;

    public PartInitNeighborParam(int matrixId, PartitionKey partKey, NodeEdgesPair[] nodeEdgesPairs
            , int startIndex, int endIndex) {
        super(matrixId, partKey);
        this.nodeEdgesPairs = nodeEdgesPairs;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    public PartInitNeighborParam() {
        this(0, null, null, 0, 0);
    }

    public NodeEdgesPair[] getNodeEdgesPairs() {
        return nodeEdgesPairs;
    }

    @Override
    public void serialize(ByteBuf buf) {
        super.serialize(buf);

        buf.writeInt(endIndex - startIndex);

        for (int i = startIndex; i < endIndex; i++) {
            NodeEdgesPair nodeEdgesPair = nodeEdgesPairs[i];
            nodeEdgesPair.getNode().serialize(buf);

            Edge[] edges = nodeEdgesPair.getEdges();
            int numEdges = edges.length;
            buf.writeInt(numEdges);

            for (Edge edge : edges) {
                edge.serialize(buf);
            }
        }
    }

    @Override
    public void deserialize(ByteBuf buf) {
        super.deserialize(buf);

        int nodeEdgesNum = buf.readInt();

        nodeEdgesPairs = new NodeEdgesPair[nodeEdgesNum];
        for (int i = 0; i < nodeEdgesNum; i++) {
            Node node = new Node();
            node.deserialize(buf);

            int numEdges = buf.readInt();
            Edge[] edges = new Edge[numEdges];

            for (int j = 0; j < numEdges; j++) {
                Edge edge = new Edge();
                edge.deserialize(buf);
                edges[j] = edge;
            }
            nodeEdgesPairs[i] = new NodeEdgesPair(node, edges);
        }
    }

    @Override
    public int bufferLen() {
        int len = super.bufferLen();
        len += 4;
        for (NodeEdgesPair nodeEdgesPair : nodeEdgesPairs) {
            len += nodeEdgesPair.getNode().bufferLen();

            len += 4;
            for (Edge edge : nodeEdgesPair.getEdges()) {
                len += edge.bufferLen();
            }
        }
        return len;
    }
}
