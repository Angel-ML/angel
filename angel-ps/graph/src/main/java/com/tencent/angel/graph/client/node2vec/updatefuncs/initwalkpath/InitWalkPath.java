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
package com.tencent.angel.graph.client.node2vec.updatefuncs.initwalkpath;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;
import java.util.Random;
import java.util.concurrent.LinkedBlockingQueue;

public class InitWalkPath extends UpdateFunc {
  /**
   * Create a new UpdateParam
   *
   * @param param
   */
  public InitWalkPath(UpdateParam param) {
    super(param);
  }

  public InitWalkPath() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitWalkPathPartitionParam pparam = (InitWalkPathPartitionParam) partParam;
    PartitionKey partKey = pparam.getPartKey();
    ServerLongAnyRow walkPath = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(partKey, 0);
    // getRow(partKey.getMatrixId(), rowId, partKey.getPartitionId())
    ServerLongAnyRow rowNeighbor = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(
            pparam.getNeighborMatrixId(), 0, partKey.getPartitionId());
    Random rand = new Random();

    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = rowNeighbor.iterator();
    walkPath.startWrite();
    walkPath.clear();
    PathQueue.init(partKey.getPartitionId(), pparam.getMod());
    List<LinkedBlockingQueue<WalkPath>> queueList = PathQueue.getQueueList(partKey.getPartitionId());
    try {
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<IElement> entry = iter.next();
        long key = entry.getLongKey() + partKey.getStartCol();
        LongArrayElement value = (LongArrayElement) entry.getValue();

        long[] neighbor = value.getData();
        long neigh = neighbor[rand.nextInt(neighbor.length)];

        WalkPath wPath = new WalkPath(pparam.getWalkLength(), pparam.getMod(), key, neigh);
        walkPath.set(key, wPath);
        PathQueue.pushPath(queueList, wPath);

//        StringBuilder sb = new StringBuilder();
//        sb.append(key).append(" -> {");
//        for (long n: neighbor) {
//          sb.append(n).append(", ");
//        }
//        sb.append("} : ").append(neigh);
//        System.out.println(sb.toString());
      }
    } finally {
      walkPath.endWrite();
    }

    System.out.println("finished init PathQueue");
  }
}
