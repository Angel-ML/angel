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

package com.tencent.angel.graph.model.neighbor.simple.psf.get;

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.common.psf.result.GetLongsResult;
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.graph.utils.GeneralGetUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.twitter.chill.ScalaKryoInstantiator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * Sample the neighbor
 */
public class GetByteNeighbor extends GetFunc {

  public static final long[] emptyLongs = new long[0];

  public GetByteNeighbor(LongKeysGetParam param) {
    super(param);
  }

  public GetByteNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return GeneralGetUtils.partitionGet(psContext, partParam);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGeneralGetResult) result).getNodeIds().length;
    }

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGeneralGetResult partResult = (PartGeneralGetResult) result;
      long[] nodeIds = partResult.getNodeIds();
      IElement[] data = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        if (data[i] != null) {
          byte[] serializedNeighbors = ((ByteArrayElement) data[i]).getData();
          if (serializedNeighbors.length > 0) {
            nodeIdToNeighbors.put(nodeIds[i],
                ScalaKryoInstantiator.defaultPool().fromBytes(serializedNeighbors, long[].class));
          } else {
            nodeIdToNeighbors.put(nodeIds[i], emptyLongs);
          }
        } else {
          nodeIdToNeighbors.put(nodeIds[i], emptyLongs);
        }
      }
    }

    return new GetLongsResult(nodeIdToNeighbors);
  }
}
