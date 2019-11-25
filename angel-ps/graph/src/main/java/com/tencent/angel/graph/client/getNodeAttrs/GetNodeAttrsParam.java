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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * The parameter of SampleNeighbor
 */
public class GetNodeAttrsParam extends GetParam {

    /**
     * Node ids
     */
    private final long[] nodeIds;

    /**
     * Sample neighbor number, if count <= 0, means get all neighbors
     */
    private final int count;

    private final List<PartitionGetParam> partParams;


    public GetNodeAttrsParam(int matrixId, long[] nodeIds, int count) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.count = count;
        this.partParams = new ArrayList<>();
    }

    public GetNodeAttrsParam() {
        this(-1, null, -1);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public int getCount() {
        return count;
    }

    public List<PartitionGetParam> getPartParams() {
        return partParams;
    }

    @Override
    public List<PartitionGetParam> split() {
        Arrays.sort(nodeIds);

        List<PartitionKey> partitions =
                PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

        if (!RowUpdateSplitUtils.isInRange(nodeIds, partitions)) {
            throw new AngelException(
                    "node id is not in range [" + partitions.get(0).getStartCol() + ", " + partitions
                            .get(partitions.size() - 1).getEndCol());
        }

        int nodeIndex = 0;
        int partIndex = 0;
        while (nodeIndex < nodeIds.length || partIndex < partitions.size()) {
            int length = 0;
            long endOffset = partitions.get(partIndex).getEndCol();
            while (nodeIndex < nodeIds.length && nodeIds[nodeIndex] < endOffset) {
                nodeIndex++;
                length++;
            }

            if (length > 0) {
                partParams.add(new PartGetNodeAttrsParam(matrixId,
                        partitions.get(partIndex), count, nodeIds, nodeIndex - length, nodeIndex));
            }
            partIndex++;
        }

        return partParams;
    }
}
