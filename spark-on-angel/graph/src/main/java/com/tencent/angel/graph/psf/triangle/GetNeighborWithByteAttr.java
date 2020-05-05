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

import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetNeighborWithByteAttr extends GetFunc {

    /**
     * Create a new DefaultGetFunc.
     *
     * @param param parameter of get udf
     */
    public GetNeighborWithByteAttr(GetParam param) {
        super(param);
    }

    public GetNeighborWithByteAttr() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartSampleNeighborWithAttrParam param = (PartSampleNeighborWithAttrParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
        long[] nodeIds = param.getNodeIds();

        NeighborsAttrsCompressedElement[] elementsCompressed = new NeighborsAttrsCompressedElement[nodeIds.length];

        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];
            // Get node neighbors
            NeighborsAttrsCompressedElement elem = (NeighborsAttrsCompressedElement) (row.get(nodeId));

            if (elem == null) {
                elementsCompressed[i] = null;
            } else {
                elementsCompressed[i] = elem;
            }
        }

        return new PartGetNeighborWithAttrResult(part.getPartitionKey().getPartitionId(), elementsCompressed);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {

        Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
                partResults.size());
        for (PartitionGetResult result : partResults) {
            partIdToResultMap.put(((PartGetNeighborWithAttrResult) result).getPartId(), result);
        }

        GetNeighborWithByteAttrParam param = (GetNeighborWithByteAttrParam) getParam();
        long[] nodeIds = param.getNodeIds();
        List<PartitionGetParam> partParams = param.getPartParams();
        Long2ObjectOpenHashMap<NeighborsAttrsCompressedElement> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

        for (PartitionGetParam partParam : partParams) {
            int start = ((PartSampleNeighborWithAttrParam) partParam).getStartIndex();
            int end = ((PartSampleNeighborWithAttrParam) partParam).getEndIndex();
            PartGetNeighborWithAttrResult partResult = (PartGetNeighborWithAttrResult) (partIdToResultMap
                    .get(partParam.getPartKey().getPartitionId()));

            NeighborsAttrsCompressedElement[] results = partResult.getNeighborsCompressed();
            for (int i = start; i < end; i++) {
                nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
            }
        }

        return new GetNeighborWithByteAttrResult(nodeIdToNeighbors);
    }
}
