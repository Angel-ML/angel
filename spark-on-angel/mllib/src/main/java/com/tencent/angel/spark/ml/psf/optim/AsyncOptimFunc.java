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
package com.tencent.angel.spark.ml.psf.optim;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRows;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;

import java.util.List;

public abstract class AsyncOptimFunc extends IncrementRows {

  public AsyncOptimFunc(UpdateParam param) {
    super(param);
  }

  public AsyncOptimFunc() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    PartAsyncOptimParam param = (PartAsyncOptimParam) partParam;
    List<RowUpdateSplit> updates = param.getUpdates();
    double[] doubles = param.getDoubles();
    int[] ints = param.getInts();
    int offset = ints[0];
    int slots = ints[1];
    Vector[] vectors = new Vector[slots];
    for (RowUpdateSplit update: updates) {
      for (int i = 0; i < slots; i++) {
        vectors[i] = getVector(param.getMatrixId(), update.getRowId() + offset * i,
          param.getPartKey());
      }

      update(vectors, update.getVector(), doubles, ints);
    }
  }

  public abstract void update(Vector[] vectors, Vector grad, double[] doubles, int[] ints);
}
