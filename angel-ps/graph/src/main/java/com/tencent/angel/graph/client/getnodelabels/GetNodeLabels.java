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

package com.tencent.angel.graph.client.getnodelabels;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.FloatArrayElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import java.util.Random;

/**
 * Sample the neighbor
 */
public class GetNodeLabels extends GetFunc {

    public GetNodeLabels(GetNodeLabelsParam param) {
        super(param);
    }

    public GetNodeLabels() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartGetNodeLabelsParam param = (PartGetNodeLabelsParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
        long[] nodeIds = param.getNodeIds();
        float[][] labels = new float[nodeIds.length][];

        int count = param.getCount();
        Random r = new Random();

        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];

            // Get node neighbor number
            Node element = (Node) (row.get(nodeId));
            if (element == null) {
                labels[i] = null;
            } else {
                float[] nodeLabels = element.getLabels();
                IntFloatVector nodeFeats = element.getFeats();
                if (nodeLabels == null || nodeLabels.length == 0 || nodeFeats == null
                        || nodeFeats.size() == 0) {
                    labels[i] = null;
                } else if (count <= 0 || nodeLabels.length <= count) {
                    labels[i] = nodeLabels;
                } else {
                    labels[i] = new float[count];
                    // If the neighbor number > count, just copy a range of neighbors to the result array, the copy position is random
                    int startPos = Math.abs(r.nextInt()) % nodeLabels.length;
                    if (startPos + count <= nodeLabels.length) {
                        System.arraycopy(nodeLabels, startPos, labels[i], 0, count);
                    } else {
                        System
                                .arraycopy(nodeLabels, startPos, labels[i], 0,
                                        nodeLabels.length - startPos);
                        System.arraycopy(nodeLabels, 0, labels[i],
                                nodeLabels.length - startPos, count - (nodeLabels.length - startPos));
                    }
                }
            }
        }

        return new PartGetNodeLabelsResult(part.getPartitionKey().getPartitionId(), labels);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
                partResults.size());
        for (PartitionGetResult result : partResults) {
            partIdToResultMap.put(((PartGetNodeLabelsResult) result).getPartId(), result);
        }

        GetNodeLabelsParam param = (GetNodeLabelsParam) getParam();
        long[] nodeIds = param.getNodeIds();
        List<PartitionGetParam> partParams = param.getPartParams();

        Long2ObjectOpenHashMap<float[]> nodeIdToLabels = new Long2ObjectOpenHashMap<>(nodeIds.length);

        for (PartitionGetParam partParam : partParams) {
            int start = ((PartGetNodeLabelsParam) partParam).getStartIndex();
            int end = ((PartGetNodeLabelsParam) partParam).getEndIndex();
            PartGetNodeLabelsResult partResult = (PartGetNodeLabelsResult) (partIdToResultMap
                    .get(partParam.getPartKey().getPartitionId()));
            float[][] results = partResult.getNodeIdToLabels();
            for (int i = start; i < end; i++) {
                nodeIdToLabels.put(nodeIds[i], results[i - start]);
            }
        }

        return new GetNodeLabelsResult(nodeIdToLabels);
    }
}
