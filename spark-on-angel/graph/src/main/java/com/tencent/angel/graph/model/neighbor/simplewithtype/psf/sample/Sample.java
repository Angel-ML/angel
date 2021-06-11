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

package com.tencent.angel.graph.model.neighbor.simplewithtype.psf.sample;

import com.tencent.angel.graph.common.psf.result.GetLongsResult;
import com.tencent.angel.graph.common.psf.result.PartGetLongsResult;
import com.tencent.angel.graph.model.neighbor.simplewithtype.TypeNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2IntMap.Entry;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
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
    KeyPart keyPart = param.getIndicesPart();
    int sampleType = param.getSampleType();

    long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
    Long2IntOpenHashMap nodeIdToSizes = new Long2IntOpenHashMap(nodeIds.length);
    for (int i = 0; i < nodeIds.length; i++) {
      nodeIdToSizes.addTo(nodeIds[i], 1);
    }

    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
    Random r = new Random();

    long[] distinctNodeIds = new long[nodeIdToSizes.size()];
    long[][] samples = new long[nodeIdToSizes.size()][];
    ObjectIterator<Entry> iter = nodeIdToSizes.long2IntEntrySet().fastIterator();
    int index = 0;
    while (iter.hasNext()) {
      Entry entry = iter.next();
      distinctNodeIds[index] = entry.getLongKey();
      TypeNeighborElement element = (TypeNeighborElement) row.get(distinctNodeIds[index]);
      samples[index] = element.sample(sampleType, r, distinctNodeIds[index], entry.getIntValue());
      index++;
    }

    return new PartGetLongsResult(distinctNodeIds, samples);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGetLongsResult) result).getData().length;
    }

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGetLongsResult partResult = (PartGetLongsResult) result;
      long[] nodeIds = partResult.getNodeIds();
      long[][] samples = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        nodeIdToNeighbors.put(nodeIds[i], samples[i]);
      }
    }

    return new GetLongsResult(nodeIdToNeighbors);
  }
}


