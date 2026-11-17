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
package com.tencent.angel.graph.client.node2vec.updatefuncs.clearandinit;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.node2vec.data.WalkPath;
import com.tencent.angel.graph.client.node2vec.utils.PathQueue;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import java.util.ArrayList;

public class ClearAndInitWalkPath extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public ClearAndInitWalkPath(ClearAndInitWalkPathUpdateParam param) {
    super(param);
  }

  public ClearAndInitWalkPath() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    ClearAndInitWalkPathUpdatePartitionParam params = (ClearAndInitWalkPathUpdatePartitionParam) partParam;
    PartitionKey partKey = params.getPartKey();
    ServerLongAnyRow walkPath = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(partKey, 0);

    ObjectIterator<Long2ObjectMap.Entry<IElement>> iter = walkPath.iterator();
    walkPath.startWrite();
    PathQueue.clear(partKey.getPartitionId());
    try {
      int count = 0;
      int batchSize = 1024;
      ArrayList<WalkPath> batchPath = new ArrayList<>();
      while (iter.hasNext()) {
        Long2ObjectMap.Entry<IElement> entry = iter.next();
        WalkPath wPath = (WalkPath) entry.getValue();

        batchPath.add(wPath);
        count++;

        if (count % batchSize == 0) {
          PathQueue.initPushBatch(partKey.getPartitionId(), batchPath);
          batchPath.clear();
        }
      }

      if (!batchPath.isEmpty()) {
        PathQueue.initPushBatch(partKey.getPartitionId(), batchPath);
        batchPath.clear();
      }
    } finally {
      walkPath.endWrite();
    }
  }
}
