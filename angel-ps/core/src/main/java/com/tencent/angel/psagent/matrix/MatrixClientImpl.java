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
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.TIntDoubleVector;
import com.tencent.angel.ml.math.vector.TLongDoubleVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.IndexGetFunc;
import com.tencent.angel.ml.matrix.psf.get.enhance.indexed.LongIndexGetFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.enhance.VoidResult;
import com.tencent.angel.psagent.PSAgent;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MatrixClientImpl extends MatrixClient {
  public MatrixClientImpl() {

  }

  @Override
  public void increment(TVector row) throws AngelException {
    increment(row, false);
  }

  @Override public void increment(TVector row, boolean disableCache) throws AngelException {
    try {
      row.setMatrixId(matrixId);
      if(disableCache) {
        PSAgentContext.get().getMatrixClientAdapter().increment(matrixId, row.getRowId(), row);
      } else {
        PSAgentContext.get().getOpLogCache().increment(taskContext, row);
      }
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public void increment(TMatrix matrix) throws AngelException {
    increment(matrix, false);
  }

  @Override public void increment(TMatrix matrix, boolean disableCache) throws AngelException {
    try {
      matrix.setMatrixId(matrixId);
      if(disableCache) {
        PSAgentContext.get().getMatrixClientAdapter().increment(matrixId, matrix);
      } else {
        PSAgentContext.get().getOpLogCache().increment(taskContext, matrix);
      }
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override public void increment(TVector[] rows) throws AngelException {
    increment(rows, false);
  }

  @Override public void increment(TVector[] rows, boolean disableCache) throws AngelException {
    try {
      for(int i = 0; i < rows.length; i++) {
        rows[i].setMatrixId(matrixId);
      }

      if(disableCache) {
        PSAgentContext.get().getMatrixClientAdapter().increment(matrixId, rows);
      } else {
        for(int i = 0; i < rows.length; i++) {
          PSAgentContext.get().getOpLogCache().increment(taskContext, rows[i]);
        }
      }
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public TVector getRow(int rowIndex) throws AngelException {
    try {
      return PSAgentContext.get().getConsistencyController().getRow(taskContext, matrixId, rowIndex);
    } catch (Exception e) {
      throw new AngelException(e);
    }
  }

  @Override
  public Future<VoidResult> flush() throws AngelException {
    return PSAgentContext.get().getMatrixOpLogCache().flush(taskContext, matrixId);
  }

  @Override
  public Future<VoidResult> clock() throws AngelException {
    return clock(true);
  }

  @Override
  public Future<VoidResult> clock(boolean flushFirst) throws AngelException {
    return PSAgentContext.get().getConsistencyController().clock(taskContext, matrixId, flushFirst);
  }

  @Override
  public GetRowsResult getRowsFlow(RowIndex index, int batchSize) throws AngelException {
    index.setMatrixId(matrixId);
    try {
      return PSAgentContext.get().getConsistencyController()
          .getRowsFlow(taskContext, index, batchSize);
    } catch (Exception e) {
      throw new AngelException(e);
    }
  }

  @Override
  public void increment(int rowId, TVector row) throws AngelException {
    increment(rowId, row, false);
  }

  @Override public void increment(int rowId, TVector row, boolean disableCache)
    throws AngelException {
    row.setMatrixId(matrixId);
    row.setRowId(rowId);
    increment(row, disableCache);
  }

  @Override
  public Future<VoidResult> update(UpdateFunc func) throws AngelException {
    return PSAgentContext.get().getMatrixClientAdapter().update(func);
  }

  @Override
  public GetResult get(GetFunc func) throws AngelException {
    try {
      return PSAgentContext.get().getMatrixClientAdapter().get(func);
    } catch (InterruptedException | ExecutionException e) {
      throw new AngelException(e);
    }
  }

  public TIntDoubleVector getRow(IndexGetFunc func) throws AngelException {
    try {
      return PSAgentContext.get().getConsistencyController().getRow(taskContext, func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  public TLongDoubleVector getRow(LongIndexGetFunc func) throws AngelException {
    try {
      return PSAgentContext.get().getConsistencyController().getRow(taskContext, func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }
}
