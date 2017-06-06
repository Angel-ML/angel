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

package com.tencent.angel.psagent.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.updater.base.UpdaterFunc;
import com.tencent.angel.ml.matrix.psf.updater.base.VoidResult;
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
    row.setMatrixId(matrixId);
    PSAgentContext.get().getOpLogCache().increment(taskContext, row);
  }

  @Override
  public void increment(TMatrix matrix) throws AngelException {
    PSAgentContext.get().getOpLogCache().increment(taskContext, matrix);
  }

  @Override
  public TVector getRow(int rowIndex) throws AngelException {
    try {
      return PSAgentContext.get().getConsistencyController()
          .getRow(taskContext, matrixId, rowIndex);
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
  public void increment(int rowIndex, TVector delta) throws AngelException {
    delta.setRowId(rowIndex);
    increment(delta);
  }

  @Override
  public Future<VoidResult> update(UpdaterFunc func) throws AngelException {
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
}
