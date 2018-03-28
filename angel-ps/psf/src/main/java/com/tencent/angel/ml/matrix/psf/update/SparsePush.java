/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.tencent.angel.ml.matrix.psf.update;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.common.CommonFunc;
import com.tencent.angel.ml.matrix.psf.common.CommonParam;
import com.tencent.angel.ml.matrix.psf.update.enhance.PartitionUpdateParam;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;

/**
 * `Push` the `values` to `rowId` row
 */
public class SparsePush extends CommonFunc {

  public SparsePush(int matrixId, int rowId, long[] indices, double[] values) {
    super(new CommonParam(matrixId, new int[]{rowId}, indices, values));
  }

  public SparsePush() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow[] rows, PartitionKey key, PartitionUpdateParam param) {
    throw new RuntimeException("SparsePush PSF can not support dense type rows");
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow[] rows, PartitionKey key, PartitionUpdateParam param) {
    CommonParam.PSFPartitionUpdateParam partParam = (CommonParam.PSFPartitionUpdateParam)param;

    int rowId = partParam.getInts()[0];
    long[] indices = partParam.getLongs();
    double[] values = partParam.getDoubles();
    ServerSparseDoubleLongKeyRow row = rows[rowId];

    Long2DoubleOpenHashMap data = new Long2DoubleOpenHashMap(indices, values);
    row.setIndex2ValueMap(data);
  }

}
