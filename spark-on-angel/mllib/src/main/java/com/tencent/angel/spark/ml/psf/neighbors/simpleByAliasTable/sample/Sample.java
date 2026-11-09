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

package com.tencent.angel.spark.ml.psf.neighbors.simpleByAliasTable.sample;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.spark.ml.psf.neighbors.simpleByAliasTable.sampleAliasTable.NeighborsAliasTableElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.util.List;
import java.util.Random;

/**
 * Sample the neighbor
 */
public class Sample extends GetFunc {

    public Sample(SampleParam param) {
        super(param);
    }

    public Sample() {
        this(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        PartSampleParam param = (PartSampleParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
        long[] nodeIds = param.getNodeIds();
        long[] samples = new long[nodeIds.length];

        Random r = new Random();

        for (int i = 0; i < nodeIds.length; i++) {
            long nodeId = nodeIds[i];

            // Get node neighbor number
            NeighborsAliasTableElement element = (NeighborsAliasTableElement) (row.get(nodeId));
            if (element == null) {
                samples[i] = nodeId;
            } else {
                samples[i] = element.sample(r, nodeId);
            }
        }

        return new PartSampleResult(part.getPartitionKey().getPartitionId(), samples);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
                partResults.size());
        for (PartitionGetResult result : partResults) {
            partIdToResultMap.put(((PartSampleResult) result).getPartId(), result);
        }

        SampleParam param = (SampleParam) getParam();
        long[] nodeIds = param.getNodeIds();
        List<PartitionGetParam> partParams = param.getPartParams();

        long[] nodeIdToSamples = new long[nodeIds.length];

        for (PartitionGetParam partParam : partParams) {
            int start = ((PartSampleParam) partParam).getStartIndex();
            int end = ((PartSampleParam) partParam).getEndIndex();
            PartSampleResult partResult = (PartSampleResult) (partIdToResultMap
                    .get(partParam.getPartKey().getPartitionId()));
            long[] results = partResult.getNodeIdToSamples();
            assert(end - start == results.length);
            System.arraycopy(results, 0, nodeIdToSamples, start, results.length);
        }
        return new SampleResult(nodeIdToSamples);
    }
}
