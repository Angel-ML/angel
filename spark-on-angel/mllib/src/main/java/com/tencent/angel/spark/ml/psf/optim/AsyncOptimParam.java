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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRowsParam;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AsyncOptimParam extends IncrementRowsParam {

  private double[] doubles;
  private int[] ints;

  public AsyncOptimParam(int matrixId, Vector[] updates, double[] doubles, int[] ints) {
    super(matrixId, updates);
    this.doubles = doubles;
    this.ints = ints;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    // split updates
    Map<PartitionKey, List<RowUpdateSplit>> partToSplits = new HashMap<>(getPartsNum(matrixId));
    for (int i = 0; i < updates.length; i++) {
      if (updates[i] != null) {
        mergeRowUpdateSplits(RowUpdateSplitUtils.split(updates[i],
          getParts(matrixId, updates[i].getRowId())), partToSplits);
      }
    }

    // shuffle update splits
    shuffleSplits(partToSplits);

    // generate part update splits
    List<PartitionUpdateParam> partParams = new ArrayList<>(partToSplits.size());
    for (Map.Entry<PartitionKey, List<RowUpdateSplit>> partEntry: partToSplits.entrySet()) {
      // set split context: partition key, use int key for long key vector or net
      adapt(partEntry.getKey(), partEntry.getValue());
      partParams.add(new PartAsyncOptimParam(matrixId, partEntry.getKey(), partEntry.getValue(), doubles, ints));
    }

    return partParams;
  }
}
