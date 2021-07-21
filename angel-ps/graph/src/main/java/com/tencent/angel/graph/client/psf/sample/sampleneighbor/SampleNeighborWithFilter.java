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

package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyLongValuePartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple2;

public class SampleNeighborWithFilter extends SampleNeighborWithType {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNeighborWithFilter(SampleNeighborWithFilterParam param) {
    super(param);
  }

  public SampleNeighborWithFilter() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborWithFilterParam sampleParam = (PartSampleNeighborWithFilterParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyLongValuePartOp split = (ILongKeyLongValuePartOp) sampleParam.getIndicesToFilterPart();
    long[] nodeIds = split.getKeys();
    long[] filterWithNeighKeys = split.getValues();
    long[] filterWithoutNeighKeys = sampleParam.getFilterWithoutNeighKeys();

    SampleType sampleType = sampleParam.getSampleType();
    switch (sampleType) {
      case NODE:
      case EDGE:{
        Tuple2<Long2ObjectOpenHashMap<long[]>, Long2ObjectOpenHashMap<int[]>> nodeId2SampleNeighbors = SampleUtils
            .sampleWithType(row, nodeIds, sampleType,
                filterWithNeighKeys, filterWithoutNeighKeys, System.currentTimeMillis());

        return new PartSampleNeighborResultWithType(sampleParam.getPartKey().getPartitionId(),
            nodeId2SampleNeighbors._1, nodeId2SampleNeighbors._2, sampleType);
      }

      default: {
        throw new UnsupportedOperationException("Not support sample type " + sampleType + " now");
      }
    }
  }
}
