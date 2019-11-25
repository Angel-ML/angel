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

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.FloatArrayElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.Random;

/**
 * Sample the neighbor
 */
public class GetNodeAttrs extends GetFunc {

    public GetNodeAttrs(GetNodeAttrsParam param) {
        super(param);
    }

    public GetNodeAttrs() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartGetNodeAttrsParam param = (PartGetNodeAttrsParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
        long[] nodeIds = param.getNodeIds();
        float[][] attrs = new float[nodeIds.length][];

        int count = param.getCount();
        Random r = new Random();

        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];

            // Get node neighbor number
            FloatArrayElement element = (FloatArrayElement) (row.get(nodeId));
            if (element == null) {
                attrs[i] = null;
            } else {
                float[] nodeAttrs = element.getData();
                if (nodeAttrs == null || nodeAttrs.length == 0) {
                    attrs[i] = null;
                } else if (count <= 0 || nodeAttrs.length <= count) {
                    attrs[i] = nodeAttrs;
                } else {
                    attrs[i] = new float[count];
                    // If the neighbor number > count, just copy a range of neighbors to the result array, the copy position is random
                    int startPos = Math.abs(r.nextInt()) % nodeAttrs.length;
                    if (startPos + count <= nodeAttrs.length) {
                        System.arraycopy(nodeAttrs, startPos, attrs[i], 0, count);
                    } else {
                        System
                                .arraycopy(nodeAttrs, startPos, attrs[i], 0,
                                        nodeAttrs.length - startPos);
                        System.arraycopy(nodeAttrs, 0, attrs[i],
                                nodeAttrs.length - startPos, count - (nodeAttrs.length - startPos));
                    }
                }
            }
        }

        return new PartGetNodeAttrsResult(part.getPartitionKey().getPartitionId(), attrs);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
                partResults.size());
        for (PartitionGetResult result : partResults) {
            partIdToResultMap.put(((PartGetNodeAttrsResult) result).getPartId(), result);
        }

        GetNodeAttrsParam param = (GetNodeAttrsParam) getParam();
        long[] nodeIds = param.getNodeIds();
        List<PartitionGetParam> partParams = param.getPartParams();

        Long2ObjectOpenHashMap<float[]> nodeIdToAttrs = new Long2ObjectOpenHashMap<>(nodeIds.length);

        for (PartitionGetParam partParam : partParams) {
            int start = ((PartGetNodeAttrsParam) partParam).getStartIndex();
            int end = ((PartGetNodeAttrsParam) partParam).getEndIndex();
            PartGetNodeAttrsResult partResult = (PartGetNodeAttrsResult) (partIdToResultMap
                    .get(partParam.getPartKey().getPartitionId()));
            float[][] results = partResult.getNodeIdToAttrs();
            for (int i = start; i < end; i++) {
                nodeIdToAttrs.put(nodeIds[i], results[i - start]);
            }
        }

        return new GetNodeAttrsResult(nodeIdToAttrs);
    }
}
