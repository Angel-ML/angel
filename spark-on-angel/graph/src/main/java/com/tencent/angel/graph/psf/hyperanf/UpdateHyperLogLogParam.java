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
package com.tencent.angel.graph.psf.hyperanf;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;
import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;

public class UpdateHyperLogLogParam extends UpdateParam {

  private Long2ObjectOpenHashMap<HyperLogLogPlus> updates;
  private int p;
  private int sp;

  public UpdateHyperLogLogParam(int matrixId, Long2ObjectOpenHashMap<HyperLogLogPlus> updates,
                                int p, int sp) {
    super(matrixId);
    this.updates = updates;
    this.p = p;
    this.sp = sp;
  }

  @Override
  public List<PartitionUpdateParam> split() {

    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] splits = splitHLLMap(meta, updates);
    assert parts.length == splits.length;
    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new UpdateHyperLogLogPartParam(matrixId, parts[i], splits[i], p, sp));
      }
    }

    return partParams;
  }

  public KeyValuePart[] splitHLLMap(MatrixMeta matrixMeta, Long2ObjectOpenHashMap<HyperLogLogPlus> data) {
    int len = data.size();
    long[] keys = new long[len];
    IElement[] values = new IElement[len];

    if (data != null && data.size() > 0) {
      ObjectIterator<Entry<Long, HyperLogLogPlus>> iter =  data.entrySet().iterator();
      int index = 0;
      while (iter.hasNext()) {
        Entry<Long, HyperLogLogPlus> entry = iter.next();
        keys[index] = entry.getKey();
        values[index] = new HLLPlusElement(entry.getValue());
        index += 1;
      }

      return RouterUtils.split(matrixMeta, 0, keys, values);
    } else {
      KeyValuePart[] keyValueParts = new KeyValuePart[matrixMeta.getPartitionNum()];
      return keyValueParts;
    }
  }

}
