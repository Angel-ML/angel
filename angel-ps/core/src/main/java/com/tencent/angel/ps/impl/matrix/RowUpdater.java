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

package com.tencent.angel.ps.impl.matrix;

import com.tencent.angel.exception.ServerRowNotFoundException;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * The base of Row updater.
 */
public abstract class RowUpdater implements RowUpdaterInterface {
  private static final Log LOG = LogFactory.getLog(RowUpdater.class);
  @Override
  public void update(RowType updateRowType, ByteBuf dataBuf, ServerRow row)
      throws Exception {
    if (row instanceof ServerSparseDoubleRow) {
      updateDoubleSparse(updateRowType, dataBuf, (ServerSparseDoubleRow) row);
    } else if (row instanceof ServerDenseDoubleRow) {
      updateDoubleDense(updateRowType, dataBuf, (ServerDenseDoubleRow) row);
    } else if (row instanceof ServerSparseDoubleLongKeyRow) {
      updateDoubleSparseLongKey(updateRowType, dataBuf, (ServerSparseDoubleLongKeyRow) row);
    } else if (row instanceof ServerSparseIntRow) {
      updateIntSparse(updateRowType, dataBuf, (ServerSparseIntRow) row);
    } else if (row instanceof ServerDenseIntRow) {
      updateIntDense(updateRowType, dataBuf, (ServerDenseIntRow) row);
    } else if (row instanceof ServerArbitraryIntRow) {
      updateIntArbitrary(updateRowType, dataBuf, (ServerArbitraryIntRow) row);
    } else if (row instanceof ServerDenseFloatRow) {
      updateFloatDense(updateRowType, dataBuf, (ServerDenseFloatRow) row);
    } else if (row instanceof ServerSparseFloatRow) {
      updateFloatSparse(updateRowType, dataBuf, (ServerSparseFloatRow) row);
    } else
      throw new ServerRowNotFoundException(row.getClass().getName());
    row.updateRowVersion();
  }

  /**
   * Update int dense data  to int arbitrary row.
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntDenseToIntArbitrary(ByteBuf dataBuf, ServerArbitraryIntRow row);

  /**
   * Update int sparse data to int arbitrary row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntSparseToIntArbitrary(ByteBuf dataBuf, ServerArbitraryIntRow row);

  /**
   * Update int dense data to int dense row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntDenseToIntDense(ByteBuf dataBuf, ServerDenseIntRow row);

  /**
   * Update int sparse data to int dense row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntSparseToIntDense(ByteBuf dataBuf, ServerDenseIntRow row);

  /**
   * Update int dense data to int sparse row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntDenseToIntSparse(ByteBuf dataBuf, ServerSparseIntRow row);

  /**
   * Update int sparse data to int sparse row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntSparseToIntSparse(ByteBuf dataBuf, ServerSparseIntRow row);

  /**
   * Update double dense to double dense.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleDenseToDoubleDense(ByteBuf dataBuf, ServerDenseDoubleRow row);

  /**
   * Update double sparse to double dense.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleSparseToDoubleDense(ByteBuf dataBuf, ServerDenseDoubleRow row);

  /**
   * Update double dense data to double sparse row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleDenseToDoubleSparse(ByteBuf dataBuf, ServerSparseDoubleRow row);

  /**
   * Update double sparse data to double sparse row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleSparseToDoubleSparse(ByteBuf dataBuf, ServerSparseDoubleRow row);

  /**
   * Update double sparse data with long key to double sparse  row with long key.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleSparseToDoubleSparseLongKey(ByteBuf dataBuf, ServerSparseDoubleLongKeyRow row);

  /**
   * Update float dense data to float dense row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatDenseToFloatDense(ByteBuf dataBuf, ServerDenseFloatRow row);

  /**
   * Update float sparse data to float dense row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatSparseToFloatDense(ByteBuf dataBuf, ServerDenseFloatRow row);

  /**
   * Update float dense data to float sparse row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatDenseToFloatSparse(ByteBuf dataBuf, ServerSparseFloatRow row);

  /**
   * Update float sparse data to float sparse row.
   *
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatSparseToFloatSparse(ByteBuf dataBuf, ServerSparseFloatRow row);

  private void updateIntArbitrary(RowType updateRowType, ByteBuf dataBuf,
      ServerArbitraryIntRow row){
    switch (updateRowType) {
      case T_INT_SPARSE:
        updateIntSparseToIntArbitrary(dataBuf, row);
        break;
      case T_INT_DENSE:
        updateIntDenseToIntArbitrary(dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateIntDense(RowType updateRowType, ByteBuf dataBuf,
      ServerDenseIntRow row){
    switch (updateRowType) {
      case T_INT_SPARSE:
        updateIntSparseToIntDense(dataBuf, row);
        break;
      case T_INT_DENSE:
        updateIntDenseToIntDense(dataBuf, row);
        break;

      default:
        break;
    }
  }


  private void updateIntSparse(RowType updateRowType, ByteBuf dataBuf,
      ServerSparseIntRow row){
    switch (updateRowType) {
      case T_INT_SPARSE:
        updateIntSparseToIntSparse(dataBuf, row);
        break;
      case T_INT_DENSE:
        updateIntDenseToIntSparse(dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateDoubleDense(RowType updateRowType, ByteBuf dataBuf,
      ServerDenseDoubleRow row){
    switch (updateRowType) {
      case T_DOUBLE_SPARSE:
        updateDoubleSparseToDoubleDense(dataBuf, row);
        break;
      case T_DOUBLE_DENSE:
        updateDoubleDenseToDoubleDense(dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateDoubleSparse(RowType updateRowType, ByteBuf dataBuf,
      ServerSparseDoubleRow row) {
    switch (updateRowType) {
      case T_DOUBLE_SPARSE:
        updateDoubleSparseToDoubleSparse(dataBuf, row);
        break;
      case T_DOUBLE_DENSE:
        updateDoubleDenseToDoubleSparse(dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateDoubleSparseLongKey(RowType updateRowType, ByteBuf dataBuf,
    ServerSparseDoubleLongKeyRow row){
    switch (updateRowType) {
      case T_DOUBLE_SPARSE_LONGKEY:
        updateDoubleSparseToDoubleSparseLongKey(dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateFloatDense(RowType updateRowType, ByteBuf dataBuf,
      ServerDenseFloatRow row){
    switch (updateRowType) {
      case T_FLOAT_DENSE:
        updateFloatDenseToFloatDense(dataBuf, row);
        break;

      case T_FLOAT_SPARSE:
        updateFloatSparseToFloatDense(dataBuf, row);

      default:
        break;
    }
  }

  private void updateFloatSparse(RowType updateRowType, ByteBuf dataBuf,
      ServerSparseFloatRow row){
    switch (updateRowType) {
      case T_FLOAT_DENSE:
        updateFloatDenseToFloatSparse(dataBuf, row);
        break;
      case T_FLOAT_SPARSE:
        updateFloatSparseToFloatSparse(dataBuf, row);
        break;

      default:
        break;
    }
  }
}
