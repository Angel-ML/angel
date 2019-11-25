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
package com.tencent.angel.graph.client.node2vec.updatefuncs.pushpathtail;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

import java.util.List;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

public class PushPathTail extends UpdateFunc {

  /**
   * Create a new UpdateParam
   *
   * @param param
   */
  public PushPathTail(UpdateParam param) {
    super(param);
  }

  public PushPathTail() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PushPathTailPartitionParam pparam = (PushPathTailPartitionParam) partParam;
    PartitionKey partKey = pparam.getPartKey();
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(pparam.getPartKey(), 0);
    Long2LongOpenHashMap pathTail = pparam.getPathTail();

    row.startWrite();
    try {
      ObjectIterator<Map.Entry<Long, Long>> iter = pathTail.entrySet().iterator();
      List<LinkedBlockingQueue<WalkPath>> queueList = PathQueue.getQueueList(partKey.getPartitionId());
      // System.out.println("pushed size: " + pathTail.size());
      while (iter.hasNext()) {
        Map.Entry<Long, Long> entry = iter.next();
        long key = entry.getKey();
        long tail = entry.getValue();

        WalkPath wPath = (WalkPath) row.get(key);
        wPath.add2Path(tail);

        if (wPath.isComplete()) {
          PathQueue.progressOne(partKey.getPartitionId());
        } else {
          PathQueue.pushPath(queueList, wPath);
        }
      }
    } finally {
      row.endWrite();
    }

    // List<LinkedBlockingQueue<WalkPath>> queueList = PathQueue.getQueueList(partKey.getPartitionId());
    // int p = 0;
    // for (LinkedBlockingQueue<WalkPath> queue: queueList) {
    //   System.out.println("partition " + p + ", size1 = "+ pathTail.size() +  " size2 = " + queue.size());
    //  p++;
    //}

    // System.out.println("pushed batch finished!");
  }
}
