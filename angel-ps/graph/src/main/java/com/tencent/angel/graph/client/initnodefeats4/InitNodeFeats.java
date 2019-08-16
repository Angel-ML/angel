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
package com.tencent.angel.graph.client.initnodefeats4;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitNodeFeats extends UpdateFunc {

  public InitNodeFeats(InitNodeFeatsParam param) {
    super(param);
  }

  public InitNodeFeats() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitNodeFeatsPartParam param = (InitNodeFeatsPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long[] nodeIds = param.getNodeIds();
    IntFloatVector[] feats = param.getFeats();

    try {
      row.startWrite();
      for (int i = 0; i < nodeIds.length; i++) {
        Node node = (Node) row.get(nodeIds[i]);
        if (node == null) {
          node = new Node();
          row.set(nodeIds[i], node);
        }

        node.setFeats(feats[i]);
      }
    } finally {
      row.endWrite();
    }
  }
}
