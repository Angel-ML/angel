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

import com.tencent.angel.ml.matrix.psf.update.enhance.CompressUpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.SparseFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.SparseParam;
import com.tencent.angel.ps.impl.matrix.ServerDenseDoubleRow;
import com.tencent.angel.ps.impl.matrix.ServerSparseDoubleLongKeyRow;
import it.unimi.dsi.fastutil.longs.Long2DoubleOpenHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.nio.DoubleBuffer;

public class SparseIncrement extends SparseFunc {
  private static final Log LOG = LogFactory.getLog(SparseIncrement.class);

  public SparseIncrement(int matrixId, int row, long[] cols, double[] values) {
    super(new SparseParam(matrixId, row, cols, values));
  }

  public SparseIncrement() {
    super();
  }

  @Override
  protected void doUpdate(ServerDenseDoubleRow row, long[] cols, double[] values) {
    row.tryToLockWrite();
    try {
      DoubleBuffer data = row.getData();
      for (int i = 0; i < cols.length; i++) {
        int tempIndex = (int)(cols[i] - row.getStartCol());
        data.put(tempIndex, data.get(tempIndex) + values[i]);
      }
    } finally {
      row.unlockWrite();
    }
  }

  @Override
  protected void doUpdate(ServerSparseDoubleLongKeyRow row, long[] cols, double[] values) {
    row.tryToLockWrite();
    try {
      long begin = System.currentTimeMillis();
      Long2DoubleOpenHashMap rowData = row.getData();
      for (int i = 0; i < cols.length; i++) {
        rowData.addTo(cols[i], values[i]);
      }
      long end = System.currentTimeMillis();
      if (end - begin > 1000) {
        LOG.info("Long2DoubleHashMap addTo time: " + (end - begin) + " ms");
        LOG.info("Long2DoubleHashMap size: " + rowData.size() + " increment size: "  + cols.length);
      }
    } finally {
      row.unlockWrite();
    }
  }

}
