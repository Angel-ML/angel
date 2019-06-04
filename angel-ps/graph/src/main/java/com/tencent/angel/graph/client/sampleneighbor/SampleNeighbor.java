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

package com.tencent.angel.graph.client.sampleneighbor;

import com.tencent.angel.graph.client.NodeIDWeightPairs;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

import java.util.List;

public class SampleNeighbor extends GetFunc {

    /**
     * Create SampleNeighbor
     *
     * @param param parameter
     */
    public SampleNeighbor(SampleNeighborParam param) {
        super(param);
    }

    /**
     * Create a empty SampleNeighbor
     */
    public SampleNeighbor() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartSampleNeighborParam param = (PartSampleNeighborParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
        GraphServerRow row = (GraphServerRow) part.getRow(0);

        // Results
        NodeIDWeightPairs[] results = new NodeIDWeightPairs[param.getNodeIds().length];

        // Get neighbors for each node
        long[] nodeIds = param.getNodeIds();
        for (int i = 0; i < nodeIds.length; i++) {
            results[i] = sampleNeighbor(row.getNode(nodeIds[i]), param.getEdgeTypes(), param.getCount());
        }

        return new PartSampleNeighborResult(part.getPartitionKey().getPartitionId(), results);
    }

    private NodeIDWeightPairs sampleNeighbor(Node node, int[] edgeTypes, int count) {
        // Edge type number to be sampled
        int edgeTypeNum = edgeTypes.length;

        // Rebuilt edge types and weights
        int[] subEdgeTypes;
        float[] subEdgeAccSumWeights;
        float subEdgeTotalSumWeights;

        if (edgeTypeNum < node.getEdgeTypes().length) {
            subEdgeTypes = new int[edgeTypeNum];
            subEdgeAccSumWeights = new float[edgeTypeNum];
            subEdgeTotalSumWeights = 0;
            for (int i = 0; i < edgeTypeNum; i++) {
                int edgeType = edgeTypes[i];
                subEdgeTypes[i] = edgeType;
                float edgeWeight = edgeType == 0 ? node.getEdgeAccSumWeights()[0] :
                        node.getEdgeAccSumWeights()[edgeType] - node.getEdgeAccSumWeights()[edgeType - 1];
                subEdgeTotalSumWeights += edgeWeight;
                subEdgeAccSumWeights[i] = subEdgeTotalSumWeights;
            }
        } else {
            subEdgeTypes = node.getEdgeTypes();
            subEdgeAccSumWeights = node.getEdgeAccSumWeights();
            subEdgeTotalSumWeights = node.getEdgeTotalSumWeights();
        }

        if (subEdgeTotalSumWeights == 0) {
            return null;
        }

        // Valid edge types
        int[] validEdgeTypes = new int[count];

        // Neighbors
        long[] neighborNodeIds = new long[count];

        // Neighbors weights
        float[] neighborWeights = new float[count];

        for (int i = 0; i < count; i++) {
            // sample edge type
            int edgeType = subEdgeTypes[randomSelect(subEdgeAccSumWeights, 0, edgeTypeNum)];

            // sample neighbor
            int startIndex = edgeType > 0 ? node.getNeigborGroupIndices()[edgeType - 1] : 0;
            int endIndex = node.getNeigborGroupIndices()[edgeType];
            int neighborNodeId = randomSelect(node.getNeighborAccSumWeights(), startIndex, endIndex);
            float preSumWeight = neighborNodeId <= 0 ? 0 : node.getNeighborAccSumWeights()[neighborNodeId - 1];

            validEdgeTypes[i] = edgeType;
            neighborNodeIds[i] = neighborNodeId;
            neighborWeights[i] = node.getNeighborAccSumWeights()[neighborNodeId] - preSumWeight;
        }

        return new NodeIDWeightPairs(validEdgeTypes, neighborNodeIds, neighborWeights);
    }

    private static int randomSelect(float[] accSumWeights, int beginPos, int endPos) {
        float limitBegin = beginPos == 0 ? 0 : accSumWeights[beginPos - 1];
        float limitEnd = accSumWeights[endPos];
        float r = (float) Math.random() * (limitEnd - limitBegin) + limitBegin;

        int low = beginPos, high = endPos, mid = 0;
        boolean finish = false;
        while (low <= high && !finish) {
            mid = (low + high) / 2;
            float intervalBegin = mid == 0 ? 0 : accSumWeights[mid - 1];
            float intervalEnd = accSumWeights[mid];
            if (intervalBegin <= r && r < intervalEnd) {
                finish = true;
            } else if (intervalBegin > r) {
                high = mid - 1;
            } else if (intervalEnd <= r) {
                low = mid + 1;
            }
        }

        return mid;
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        return null;
    }
}
