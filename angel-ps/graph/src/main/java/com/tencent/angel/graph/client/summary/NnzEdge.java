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
package com.tencent.angel.graph.client.summary;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.aggr.enhance.UnaryAggrFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.storage.LongElementStorage;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class NnzEdge extends UnaryAggrFunc {

  public NnzEdge(int matrixId, int rowId) {
    super(matrixId, rowId);
  }

  public NnzEdge() {
  }

  @Override
  public double mergeInit() {
    return 0;
  }

  @Override
  public double mergeOp(double a, double b) {
    return a + b;
  }

  @Override
  public double processRow(ServerRow row) {
    LongElementStorage storage = ((ServerLongAnyRow) row).getStorage();
    ObjectIterator<Long2ObjectMap.Entry<IElement>> it = storage.iterator();
    long size = 0;
    while (it.hasNext()) {
      Node node = (Node) (it.next().getValue());
      if (node.getNeighbors() != null) {
        size += node.getNeighbors().length;
      }
    }
    return size;
  }
}
