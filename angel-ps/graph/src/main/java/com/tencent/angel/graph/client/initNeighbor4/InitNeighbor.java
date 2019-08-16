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
package com.tencent.angel.graph.client.initNeighbor4;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.Arrays;
import java.util.Random;

public class InitNeighbor extends UpdateFunc {

  public InitNeighbor(InitNeighborParam param) {
    super(param);
  }

  public InitNeighbor() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartInitNeighborParam param = (PartInitNeighborParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

    ObjectIterator<Long2ObjectMap.Entry<long[]>> it = param.getNodesToNeighbors()
      .long2ObjectEntrySet().iterator();

    Random random = new Random(System.currentTimeMillis());

    row.startWrite();
    try {
      while (it.hasNext()) {
        Long2ObjectMap.Entry<long[]> entry = it.next();
        Node node = (Node) row.get(entry.getLongKey());
        if (node == null) {
          node = new Node();
          row.set(entry.getLongKey(), node);
        }
        long[] neighbor = entry.getValue();
        LongArrays.shuffle(neighbor, random);
        node.setNeighbors(neighbor);
      }
    } finally {
      row.endWrite();
    }
  }
}
