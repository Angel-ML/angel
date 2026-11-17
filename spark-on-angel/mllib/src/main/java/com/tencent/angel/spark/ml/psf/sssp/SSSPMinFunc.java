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
package com.tencent.angel.spark.ml.psf.sssp;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ml.matrix.psf.update.update.IncrementRows;
import com.tencent.angel.ps.storage.vector.ServerRow;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class SSSPMinFunc extends IncrementRows {

  public SSSPMinFunc(UpdateParam param) {
    super(param);
  }

  public SSSPMinFunc() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {

    SSSPMinPartParam param = (SSSPMinPartParam) partParam;
    Vector update = param.getUpdates().get(0).getVector();
    int[] rowIds = param.getInts();

    // inMsgs=0, outMsgs=1, distances=2

    int outMsgsId = rowIds[0];
    int distancesId = rowIds[1];

    int matrixId = param.getMatrixId();
    PartitionKey pkey = param.getPartKey();

    ServerRow row = psContext.getMatrixStorageManager().getRow(pkey, distancesId);
    try {
      row.startWrite();
      Vector outMsgs = getVector(matrixId, outMsgsId, pkey);
      Vector distances = getVector(matrixId, distancesId, pkey);
      if (outMsgs instanceof LongFloatVector) {
        updateDistances((LongFloatVector) outMsgs, (LongFloatVector) distances,
            (LongFloatVector) update);
      } else if (outMsgs instanceof IntFloatVector) {
        updateDistances((IntFloatVector) outMsgs, (IntFloatVector) distances,
            (IntFloatVector) update);
      }
    } finally {
      row.endWrite();
    }
  }

  private void updateDistances(LongFloatVector outMsgs, LongFloatVector distances,
      LongFloatVector update) {
    ObjectIterator<Long2FloatMap.Entry> it = update.getStorage().entryIterator();

    if (distances.size() == 0) {
      distances.setStorage(update.getStorage().emptySparse((int) update.size()));
    }
    if (outMsgs.size() == 0) {
      outMsgs.setStorage(update.getStorage().emptySparse((int) update.size()));
    }

    while (it.hasNext()) {
      Long2FloatMap.Entry entry = it.next();
      long key = entry.getLongKey();
      float value = entry.getFloatValue();
      float old = Float.POSITIVE_INFINITY;
      if (distances.hasKey(key)) {
        old = distances.get(key);
      }
      if (value < old) {
        distances.set(key, value);
        outMsgs.set(key, value);
      }
    }
  }

  private void updateDistances(IntFloatVector outMsgs, IntFloatVector distances,
      IntFloatVector update) {
    ObjectIterator<Int2FloatMap.Entry> it = update.getStorage().entryIterator();

    if (distances.size() == 0) {
      distances.setStorage(update.getStorage().emptySparse(update.size()));
    }
    if (outMsgs.size() == 0) {
      outMsgs.setStorage(update.getStorage().emptySparse(update.size()));
    }

    while (it.hasNext()) {
      Int2FloatMap.Entry entry = it.next();
      int key = entry.getIntKey();
      float value = entry.getFloatValue();
      float old = Float.POSITIVE_INFINITY;
      if (distances.hasKey(key)) {
        old = distances.get(key);
      }
      if (value < old) {
        distances.set(key, value);
        outMsgs.set(key, value);
      }
    }
  }
}
