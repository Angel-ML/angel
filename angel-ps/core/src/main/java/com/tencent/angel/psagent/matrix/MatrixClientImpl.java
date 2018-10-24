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


package com.tencent.angel.psagent.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;

import java.util.concurrent.Future;

public class MatrixClientImpl extends MatrixClient {
  public MatrixClientImpl() {

  }

  @Override public void increment(Vector row) throws AngelException {
    increment(row, false);
  }

  @Override public void increment(int rowId, Vector row) throws AngelException {
    increment(rowId, row, false);
  }

  @Override public void increment(Vector row, boolean disableCache) throws AngelException {
    increment(row.getRowId(), row, disableCache);
  }

  @Override public void increment(int rowId, Vector row, boolean disableCache)
    throws AngelException {
    row.setMatrixId(matrixId);
    row.setRowId(rowId);
    try {
      if (disableCache) {
        PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowId, row, UpdateOp.PLUS)
          .get();
      } else {
        PSAgentContext.get().getOpLogCache().increment(taskContext, row);
      }
    } catch (Throwable e) {
      throw new AngelException("increment failed ", e);
    }
  }

  @Override public void increment(Matrix matrix) throws AngelException {
    increment(matrix, false);
  }

  @Override public void increment(Matrix delta, boolean disableCache) throws AngelException {
    delta.setMatrixId(matrixId);
    try {
      if (disableCache) {
        PSAgentContext.get().getUserRequestAdapter().update(matrixId, delta, UpdateOp.PLUS).get();
      } else {
        PSAgentContext.get().getOpLogCache().increment(taskContext, delta);
      }
    } catch (Throwable e) {
      throw new AngelException("increment failed ", e);
    }
  }

  @Override public void increment(int[] rowIds, Vector[] rows) throws AngelException {
    increment(rowIds, rows, false);
  }

  @Override public void increment(int[] rowIds, Vector[] rows, boolean disableCache)
    throws AngelException {
    assert rowIds.length == rows.length;
    for (int i = 0; i < rows.length; i++) {
      rows[i].setMatrixId(matrixId);
      rows[i].setRowId(rowIds[i]);
    }

    try {
      if (disableCache) {
        PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowIds, rows, UpdateOp.PLUS)
          .get();
      } else {
        PSAgentContext.get().getOpLogCache().increment(taskContext, rows);
      }
    } catch (Throwable e) {
      throw new AngelException("increment failed ", e);
    }
  }

  @Override public void update(int rowId, Vector row) throws AngelException {
    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowId, row, UpdateOp.REPLACE)
        .get();
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override public void update(Vector row) throws AngelException {
    update(row.getRowId(), row);
  }

  @Override public void update(Matrix delta) throws AngelException {
    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, delta, UpdateOp.REPLACE).get();
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override public void update(int[] rowIds, Vector[] rows) throws AngelException {
    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowIds, rows, UpdateOp.REPLACE)
        .get();
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override public Vector get(int rowId, int[] indices) throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector get(int rowId, long[] indices) throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector[] get(int[] rowIds, int[] indices) throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector[] get(int[] rowIds, long[] indices) throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector initAndGet(int rowId, int[] indices, InitFunc func)
    throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices, func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector initAndGet(int rowId, long[] indices, InitFunc func)
    throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices, func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector[] initAndGet(int[] rowIds, int[] indices, InitFunc func)
    throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices, func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector[] initAndGet(int[] rowIds, long[] indices, InitFunc func)
    throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices, func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Future<VoidResult> update(UpdateFunc func) throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().update(func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public GetResult get(GetFunc func) throws AngelException {
    try {
      return PSAgentContext.get().getUserRequestAdapter().get(func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Vector getRow(int rowIndex) throws AngelException {
    return getRow(rowIndex, false);
  }

  @Override public Vector getRow(int rowId, boolean disableCache) throws AngelException {
    try {
      if (disableCache) {
        return PSAgentContext.get().getUserRequestAdapter().getRow(matrixId, rowId);
      } else {
        return PSAgentContext.get().getConsistencyController().getRow(taskContext, matrixId, rowId);
      }
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public GetRowsResult getRowsFlow(RowIndex index, int batchSize) throws AngelException {
    return getRowsFlow(index, batchSize, false);
  }

  @Override public GetRowsResult getRowsFlow(RowIndex index, int batchSize, boolean disableCache)
    throws AngelException {
    index.setMatrixId(matrixId);
    try {
      if (disableCache) {
        GetRowsResult result = new GetRowsResult();
        return PSAgentContext.get().getUserRequestAdapter().getRowsFlow(result, index, batchSize);
      } else {
        return PSAgentContext.get().getConsistencyController()
          .getRowsFlow(taskContext, index, batchSize);
      }
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public Future<VoidResult> flush() throws AngelException {
    return PSAgentContext.get().getMatrixOpLogCache().flush(taskContext, matrixId);
  }

  @Override public Future<VoidResult> clock() throws AngelException {
    return clock(true);
  }

  @Override public Future<VoidResult> clock(boolean flushFirst) throws AngelException {
    return PSAgentContext.get().getConsistencyController().clock(taskContext, matrixId, flushFirst);
  }
}
