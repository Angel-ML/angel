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
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import com.tencent.angel.psagent.matrix.transport.adapter.RowIndex;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class MatrixClientImpl extends MatrixClient {
  private static final Log LOG = LogFactory.getLog(MatrixClientImpl.class);

  public MatrixClientImpl() {

  }

  private void checkNotNull(Object obj, String name) {
    if (obj == null) {
      throw new AngelException("Unvalid parameter " + name + " can not be null");
    }
  }

  private void checkRowId(int rowId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if (rowId < 0 || rowId >= matrixMeta.getRowNum()) {
      throw new AngelException(
        "Unvalid parameter, row id should in [0, " + matrixMeta.getRowNum() + "), but=" + rowId);
    }
  }

  @Override
  public void increment(Vector row) throws AngelException {
    increment(row, true);
  }

  @Override
  public Future<VoidResult> asyncIncrement(Vector row) throws AngelException {
    return asyncIncrement(row.getRowId(), row);
  }

  @Override
  public void increment(int rowId, Vector row) throws AngelException {
    increment(rowId, row, true);
  }

  @Override
  public Future<VoidResult> asyncIncrement(int rowId, Vector row) throws AngelException {
    checkRowId(rowId);
    checkNotNull(row, "row");

    row.setMatrixId(matrixId);
    row.setRowId(rowId);

    return PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowId, row, UpdateOp.PLUS);
  }

  @Deprecated
  @Override
  public void increment(Vector row, boolean disableCache) throws AngelException {
    checkNotNull(row, "row");
    increment(row.getRowId(), row, disableCache);
  }


  @Deprecated
  @Override
  public void increment(int rowId, Vector row, boolean disableCache)
    throws AngelException {
    checkRowId(rowId);
    checkNotNull(row, "row");

    row.setMatrixId(matrixId);
    row.setRowId(rowId);
    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowId, row, UpdateOp.PLUS)
          .get();
    } catch (Throwable e) {
      throw new AngelException("increment failed ", e);
    }
  }

  @Override
  public void increment(Matrix matrix) throws AngelException {
    increment(matrix, true);
  }

  @Override
  public Future<VoidResult> asyncIncrement(Matrix matrix) throws AngelException {
    checkNotNull(matrix, "matrix");
    matrix.setMatrixId(matrixId);
    return PSAgentContext.get().getUserRequestAdapter().update(matrixId, matrix, UpdateOp.PLUS);
  }

  @Deprecated
  @Override
  public void increment(Matrix matrix, boolean disableCache) throws AngelException {
    checkNotNull(matrix, "matrix");

    matrix.setMatrixId(matrixId);
    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, matrix, UpdateOp.PLUS).get();
    } catch (Throwable e) {
      throw new AngelException("increment failed ", e);
    }
  }


  @Override
  public void increment(int[] rowIds, Vector[] rows) throws AngelException {
    increment(rowIds, rows, true);
  }

  @Override
  public Future<VoidResult> asyncIncrement(int[] rowIds, Vector[] rows) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(rows, "rows");

    assert rowIds.length == rows.length;

    // Just return
    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return now!!!");
      FutureResult result = new FutureResult<VoidResult>();
      result.set(new VoidResult(ResponseType.SUCCESS));
      return result;
    }

    return PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowIds, rows, UpdateOp.PLUS);
  }

  @Deprecated
  @Override
  public void increment(int[] rowIds, Vector[] rows, boolean disableCache)
    throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(rows, "rows");

    assert rowIds.length == rows.length;

    // Just return
    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return now!!!");
      return;
    }

    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowIds, rows, UpdateOp.PLUS)
          .get();
    } catch (Throwable e) {
      throw new AngelException("increment failed ", e);
    }
  }


  @Override
  public void update(int rowId, Vector row) throws AngelException {
    checkRowId(rowId);
    checkNotNull(row, "row");

    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowId, row, UpdateOp.REPLACE)
        .get();
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override
  public Future<VoidResult> asyncUpdate(int rowId, Vector row) throws AngelException {
    checkRowId(rowId);
    checkNotNull(row, "row");

    try {
      return PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowId, row, UpdateOp.REPLACE);
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override
  public void update(Vector row) throws AngelException {
    checkNotNull(row, "row");

    update(row.getRowId(), row);
  }

  @Override
  public Future<VoidResult> asyncUpdate(Vector row) throws AngelException {
    return asyncUpdate(row.getRowId(), row);
  }

  @Override
  public void update(Matrix matrix) throws AngelException {
    checkNotNull(matrix, "matrix");

    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, matrix, UpdateOp.REPLACE).get();
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override
  public Future<VoidResult> asyncUpdate(Matrix matrix) throws AngelException {
    checkNotNull(matrix, "matrix");

    try {
      return PSAgentContext.get().getUserRequestAdapter().update(matrixId, matrix, UpdateOp.REPLACE);
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override
  public void update(int[] rowIds, Vector[] rows) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(rows, "rows");

    assert rowIds.length == rows.length;

    // Just return
    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return now!!!");
      return;
    }

    try {
      PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowIds, rows, UpdateOp.REPLACE)
        .get();
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override
  public Future<VoidResult> asyncUpdate(int[] rowIds, Vector[] rows) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(rows, "rows");

    assert rowIds.length == rows.length;

    // Just return
    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return now!!!");
      FutureResult<VoidResult> result = new FutureResult<>();
      result.set(new VoidResult(ResponseType.SUCCESS));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().update(matrixId, rowIds, rows, UpdateOp.REPLACE);
    } catch (Throwable e) {
      throw new AngelException(e);
    }
  }

  @Override
  public Vector get(int rowId, int[] indices) throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      return generateEmptyVec(rowId);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector> asyncGet(int rowId, int[] indices) throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      FutureResult<Vector> result = new FutureResult<>();
      result.set(generateEmptyVec(rowId));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }


  private Vector generateEmptyVec(int rowId) {
    MatrixMeta matrixMeta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    RowType rowType = matrixMeta.getRowType();
    Vector vector;
    if (rowType.isInt()) {
      if (rowType.isLongKey())
        vector = VFactory.sparseLongKeyIntVector(0, 0);
      else
        vector = VFactory.sparseIntVector(0, 0);
    } else if (rowType.isLong()) {
      if (rowType.isLongKey())
        vector = VFactory.sparseLongKeyLongVector(0, 0);
      else
        vector = VFactory.sparseLongVector(0, 0);
    } else if (rowType.isFloat()) {
      if (rowType.isLongKey())
        vector = VFactory.sparseLongKeyFloatVector(0, 0);
      else
        vector = VFactory.sparseFloatVector(0, 0);
    } else if (rowType.isDouble()) {
      if (rowType.isLongKey())
        vector = VFactory.sparseLongKeyDoubleVector(0, 0);
      else
        vector = VFactory.sparseDoubleVector(0, 0);
    } else {
      throw new AngelException("Unsupport row type");
    }
    vector.setRowId(rowId);
    vector.setMatrixId(matrixId);
    return vector;
  }

  private Vector[] generateEmptyVecs(int[] rowIds) {
    Vector[] ret = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      ret[i] = generateEmptyVec(rowIds[i]);
    }
    return ret;
  }

  @Override
  public Vector get(int rowId, long[] indices) throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      return generateEmptyVec(rowId);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector> asyncGet(int rowId, long[] indices) throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      FutureResult<Vector> result = new FutureResult<>();
      result.set(generateEmptyVec(rowId));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector[] get(int[] rowIds, int[] indices) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      return new Vector[0];
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      return generateEmptyVecs(rowIds);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector[]> asyncGet(int[] rowIds, int[] indices) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(new Vector[0]);
      return result;
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(generateEmptyVecs(rowIds));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector[] get(int[] rowIds, long[] indices) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      return new Vector[0];
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      return generateEmptyVecs(rowIds);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector[]> asyncGet(int[] rowIds, long[] indices) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(new Vector[0]);
      return result;
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(generateEmptyVecs(rowIds));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector initAndGet(int rowId, int[] indices, InitFunc func)
    throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      return generateEmptyVec(rowId);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices, func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector> asyncInitAndGet(int rowId, int[] indices, InitFunc func)
    throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      FutureResult<Vector> result = new FutureResult<>();
      result.set(generateEmptyVec(rowId));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices, func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector initAndGet(int rowId, long[] indices, InitFunc func)
    throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      return generateEmptyVec(rowId);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices, func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector> asyncInitAndGet(int rowId, long[] indices, InitFunc func)
    throws AngelException {
    checkRowId(rowId);
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return a empty vector now!!!");
      FutureResult<Vector> result = new FutureResult<>();
      result.set(generateEmptyVec(rowId));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowId, indices, func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector[] initAndGet(int[] rowIds, int[] indices, InitFunc func)
    throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      return new Vector[0];
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      return generateEmptyVecs(rowIds);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices, func)
        .get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector[]> asyncInitAndGet(int[] rowIds, int[] indices, InitFunc func)
    throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(new Vector[0]);
      return result;
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(generateEmptyVecs(rowIds));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices, func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector[] initAndGet(int[] rowIds, long[] indices, InitFunc func)
    throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      return new Vector[0];
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      return generateEmptyVecs(rowIds);
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices, func)
        .get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<Vector[]> asyncInitAndGet(int[] rowIds, long[] indices, InitFunc func)
    throws AngelException {
    checkNotNull(rowIds, "rowIds");
    checkNotNull(indices, "indices");
    //checkNotNull(func, "func");

    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(new Vector[0]);
      return result;
    }

    // Return a empty vector
    if (indices.length == 0) {
      LOG.warn("parameter indices is empty, you should check it, just return empty vectors now!!!");
      FutureResult<Vector[]> result = new FutureResult<>();
      result.set(generateEmptyVecs(rowIds));
      return result;
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(matrixId, rowIds, indices, func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public void update(UpdateFunc func) throws AngelException {
    checkNotNull(func, "func");

    try {
      PSAgentContext.get().getUserRequestAdapter().update(func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<VoidResult> asyncUpdate(UpdateFunc func) throws AngelException {
    checkNotNull(func, "func");

    try {
      return PSAgentContext.get().getUserRequestAdapter().update(func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public GetResult get(GetFunc func) throws AngelException {
    checkNotNull(func, "func");

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(func).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Future<GetResult> asyncGet(GetFunc func) throws AngelException {
    checkNotNull(func, "func");

    try {
      return PSAgentContext.get().getUserRequestAdapter().get(func);
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Override
  public Vector getRow(int rowId) throws AngelException {
    return getRow(rowId, false);
  }

  @Override
  public Future<Vector> asyncGetRow(int rowId) throws AngelException {
    checkRowId(rowId);
    return PSAgentContext.get().getUserRequestAdapter().getRow(matrixId, rowId);
  }

  @Deprecated
  @Override
  public Vector getRow(int rowId, boolean disableCache) throws AngelException {
    checkRowId(rowId);

    try {
      return PSAgentContext.get().getUserRequestAdapter().getRow(matrixId, rowId).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }

  @Deprecated
  @Override
  public GetRowsResult getRowsFlow(RowIndex index, int batchSize) throws AngelException {
    return getRowsFlow(index, batchSize, false);
  }

  @Override
  public Vector[] getRows(int[] rowIds) throws AngelException {
    return getRows(rowIds, true);
  }


  @Deprecated
  @Override
  public Vector[] getRows(int[] rowIds, boolean disableCache) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    return getRows(rowIds, rowIds.length, disableCache);
  }

  @Override
  public Vector[] getRows(int[] rowIds, int batchSize) throws AngelException {
    return getRows(rowIds, batchSize, false);
  }

  @Deprecated
  @Override
  public Vector[] getRows(int[] rowIds, int batchSize, boolean disableCache) throws AngelException {
    checkNotNull(rowIds, "rowIds");
    if (rowIds.length == 0) {
      LOG.warn("parameter rowIds is empty, you should check it, just return a empty vector array now!!!");
      return new Vector[0];
    }

    try {
      return PSAgentContext.get().getUserRequestAdapter().getRows(matrixId, rowIds).get();
    } catch (Throwable x) {
      throw new AngelException(x);
    }
  }


  @Deprecated
  @Override
  public GetRowsResult getRowsFlow(RowIndex index, int batchSize, boolean disableCache)
    throws AngelException {
    throw new UnsupportedOperationException("Not support now");
  }

  @Deprecated
  @Override
  public Future<VoidResult> flush() throws AngelException {
    FutureResult<VoidResult> result = new FutureResult<>();
    result.set(new VoidResult(ResponseType.SUCCESS));
    return result;
    //return PSAgentContext.get().getMatrixOpLogCache().flush(taskContext, matrixId);
  }

  @Deprecated
  @Override
  public Future<VoidResult> clock() throws AngelException {
    return clock(true);
  }

  @Deprecated
  @Override
  public FutureResult<VoidResult> checkpoint(int id) throws AngelException {
    throw new UnsupportedOperationException("Not support now");
  }

  @Deprecated
  @Override
  public Future<VoidResult> clock(boolean flushFirst) throws AngelException {
    FutureResult<VoidResult> result = new FutureResult<>();
    result.set(new VoidResult(ResponseType.SUCCESS));
    return result;
    //return PSAgentContext.get().getConsistencyController().clock(taskContext, matrixId, flushFirst);
  }
}
