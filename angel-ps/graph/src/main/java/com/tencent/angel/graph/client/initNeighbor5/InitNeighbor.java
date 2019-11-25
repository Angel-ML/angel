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
package com.tencent.angel.graph.client.initNeighbor5;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

/* Mini-batch version of pushing csr neighbors */
public class InitNeighbor extends UpdateFunc {

  public InitNeighbor(InitNeighborParam param) {
    super(param);
  }

  public InitNeighbor() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitNeighborPartParam param = (InitNeighborPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0));

    long[] keys = param.getKeys();
    long[][] neighborArrays = param.getNeighborArrays();
    int[][] neighborTypes = param.getTypeArrays();

    row.startWrite();
    try {
      for (int i = 0; i < keys.length; i++) {
        Node node = (Node) row.get(keys[i]);
        if (node == null) {
          node = new Node();
          row.set(keys[i], node);
        }

        node.setNeighbors(neighborArrays[i]);
        if (neighborTypes != null)
          node.setTypes(neighborTypes[i]);
      }
    } finally {
      row.endWrite();
    }

  }

}
