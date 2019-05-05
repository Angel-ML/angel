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

package com.tencent.angel.spark.ml.psf.embedding.bad;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;

import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerIntFloatRow;
import com.tencent.angel.ps.storage.vector.ServerRowUtils;
import java.util.Random;

public class W2VRandom extends UpdateFunc {


  public W2VRandom(int matrixId, int dimension) {
    this(new W2VRandomParam(matrixId, dimension));
  }

  public W2VRandom(UpdateParam param) {
    super(param);
  }

  public W2VRandom() {
    super(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof W2VRandomPartitionParam) {
      W2VRandomPartitionParam param = (W2VRandomPartitionParam) partParam;
      int dimension = param.dimension;
      RowBasedPartition partition = (RowBasedPartition) psContext.getMatrixStorageManager()
          .getPart(param.getPartKey());
      update(partition, param.getPartKey(), dimension);
    }
  }

  private void update(RowBasedPartition partition,
      PartitionKey pkey,
      int dimension) {
    int startRow = pkey.getStartRow();
    int endRow = pkey.getEndRow();

    Random random = new Random(System.currentTimeMillis());
    for (int r = startRow; r < endRow; r++) {
      float[] values = ServerRowUtils.getVector((ServerIntFloatRow) partition.getRow(r)
      ).getStorage()
          .getValues();

      assert values.length % (dimension * 2) == 0;
      int numRows = values.length / dimension / 2;
      for (int a = 0; a < numRows; a++) {
        int offset = a * dimension * 2;
        for (int b = 0; b < dimension; b++)
//          values[b + offset] = (random.nextFloat() - 0.5f) / dimension;
        {
          values[b + offset] = 0.01f;
        }
      }
    }
  }
}
