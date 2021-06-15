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
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap;


public class PushPathTail extends UpdateFunc {

  /**
   * Create a new UpdateParam
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
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(pparam.getPartKey(), 0);
    Long2LongOpenHashMap pathTail = pparam.getPathTail();

    PathQueue.pushBatch(partKey.getPartitionId(), row, pathTail);
//    int completeCount = 0;
//    ArrayList<WalkPath> paths = new ArrayList<>(pathTail.size());
//
//    row.startWrite();
//    try {
//      ObjectIterator<Map.Entry<Long, Long>> iter = pathTail.entrySet().iterator();
//      while (iter.hasNext()) {
//        Map.Entry<Long, Long> entry = iter.next();
//        long key = entry.getKey();
//        long tail = entry.getValue();
//
//        WalkPath wPath = (WalkPath) row.get(key);
//        wPath.add2Path(tail);
//
//        if (wPath.isComplete()) {
//          completeCount += 1;
//        } else {
//          paths.add(wPath);
//        }
//      }
//    } finally {
//      row.endWrite();
//    }
//
//    if (completeCount != 0) {
//      PathQueue.progress(partKey.getPartitionId(), completeCount);
//    }
//
//    if (!paths.isEmpty()) {
//      PathQueue.pushBatch(partKey.getPartitionId(), paths);
//    }
  }
}
