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

package com.tencent.angel.graph.model.attr.psf.get;

import com.tencent.angel.graph.client.getNodeAttrs.GetNodeAttrsParam;
import com.tencent.angel.graph.common.psf.result.GetFloatsResult;
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.graph.utils.GeneralGetUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.FloatArrayElement;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * Sample the neighbor
 */
public class GetNodeAttrs extends GetFunc {
  public static final float[] emptyFloats = new float[0];

  public GetNodeAttrs(GetNodeAttrsParam param) {
    super(param);
  }

  public GetNodeAttrs() {
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

    Long2ObjectOpenHashMap<float[]> nodeIdToAttrs = new Long2ObjectOpenHashMap<>(resultSize);
    for (PartitionGetResult result : partResults) {
      PartGeneralGetResult partResult = (PartGeneralGetResult) result;
      long[] nodeIds = partResult.getNodeIds();
      IElement[] attrs = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        if(attrs[i] != null) {
          nodeIdToAttrs.put(nodeIds[i], ((FloatArrayElement)attrs[i]).getData());
        } else {
          nodeIdToAttrs.put(nodeIds[i], emptyFloats);
        }
      }
    }

    return new GetFloatsResult(nodeIdToAttrs);
  }
}
