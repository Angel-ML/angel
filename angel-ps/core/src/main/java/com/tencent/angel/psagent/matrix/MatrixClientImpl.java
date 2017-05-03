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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.psagent.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.matrix.udf.aggr.AggrFunc;
import com.tencent.angel.ml.matrix.udf.aggr.AggrResult;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowFunc;
import com.tencent.angel.ml.matrix.udf.getrow.GetRowResult;
import com.tencent.angel.ml.matrix.udf.updater.UpdaterFunc;
import com.tencent.angel.ml.matrix.udf.updater.VoidResult;
import com.tencent.angel.ml.matrix.udf.updater.ZeroUpdater;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MatrixClientImpl extends MatrixClient {
  public MatrixClientImpl() {

  }

  @Override
  public void increment(TVector row) throws AngelException{
    row.setMatrixId(matrixId);
    PSAgentContext.get().getOpLogCache().increment(taskContext, row);
  }

  @Override
  public void increment(TMatrix matrix) throws AngelException{
    PSAgentContext.get().getOpLogCache().increment(taskContext, matrix);
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
  public void zero() throws AngelException {
    ZeroUpdater updater = new ZeroUpdater(new ZeroUpdater.ZeroUpdaterParam(matrixId, false));
    try {
      update(updater).get();
    } catch (InterruptedException | ExecutionException e) {
      throw new AngelException(e);
    }
  }

  @Override
  public void increment(double[] delta) throws AngelException {
    DenseDoubleVector vector = new DenseDoubleVector(delta.length, delta);
    vector.setMatrixId(matrixId);
    vector.setRowId(0);
    vector.setClock(taskContext.getMatrixClock(matrixId));
    increment(vector);
  }

  @Override
  public void increment(int[] indexes, double[] delta) throws AngelException {
    // TODO Auto-generated method stub

  }

  @Override
  public double[] get(int[] indexes) throws AngelException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double[] get() throws AngelException {
    DenseDoubleVector vector = (DenseDoubleVector) getRow(0);
    return vector.getValues();
  }

  @Override
  public AggrResult aggr(AggrFunc func) throws AngelException {
    try {
      return PSAgentContext.get().getMatrixClientAdapter().aggr(func);
    } catch (InterruptedException | ExecutionException e) {
      throw new AngelException(e);
    }
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
  public GetRowResult getRow(GetRowFunc func) throws AngelException {
    try{
      if (func.getParam().isBypassMode()) {
        return PSAgentContext.get().getMatrixClientAdapter().getRow(func);
      } else {
        return PSAgentContext.get().getConsistencyController().getRow(taskContext, func);
      }
    } catch (Exception x) {
      throw new AngelException(x);
    }
  }
}
