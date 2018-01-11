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

/**
 * The base of Row updater.
 */
public abstract class RowUpdater implements RowUpdaterInterface {

  @Override
  public void update(RowType updateRowType, int size, ByteBuf dataBuf, ServerRow row)
      throws Exception {
    try {
      row.getLock().writeLock().lock();
      if (row instanceof ServerSparseDoubleRow) {
        updateDoubleSparse(updateRowType, size, dataBuf, (ServerSparseDoubleRow) row);
      } else if (row instanceof ServerDenseDoubleRow) {
        updateDoubleDense(updateRowType, size, dataBuf, (ServerDenseDoubleRow) row);
      } else if (row instanceof ServerSparseDoubleLongKeyRow) {
        updateDoubleSparseLongKey(updateRowType, size, dataBuf, (ServerSparseDoubleLongKeyRow) row);
      } else if (row instanceof ServerSparseIntRow) {
        updateIntSparse(updateRowType, size, dataBuf, (ServerSparseIntRow) row);
      } else if (row instanceof ServerDenseIntRow) {
        updateIntDense(updateRowType, size, dataBuf, (ServerDenseIntRow) row);
      } else if (row instanceof ServerArbitraryIntRow) {
        updateIntArbitrary(updateRowType, size, dataBuf, (ServerArbitraryIntRow) row);
      } else if (row instanceof ServerDenseFloatRow) {
        updateFloatDense(updateRowType, size, dataBuf, (ServerDenseFloatRow) row);
      } else if (row instanceof ServerSparseFloatRow) {
        updateFloatSparse(updateRowType, size, dataBuf, (ServerSparseFloatRow) row);
      } else
        throw new ServerRowNotFoundException(row.getClass().getName());
      row.updateRowVersion();
    } finally {
      row.getLock().writeLock().unlock();
    }

  }

  /**
   * Update int dense data  to int arbitrary row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntDenseToIntArbitrary(int size, ByteBuf dataBuf, ServerArbitraryIntRow row);

  /**
   * Update int sparse data to int arbitrary row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntSparseToIntArbitrary(int size, ByteBuf dataBuf, ServerArbitraryIntRow row);

  /**
   * Update int dense data to int dense row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntDenseToIntDense(int size, ByteBuf dataBuf, ServerDenseIntRow row);

  /**
   * Update int sparse data to int dense row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntSparseToIntDense(int size, ByteBuf dataBuf, ServerDenseIntRow row);

  /**
   * Update int dense data to int sparse row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntDenseToIntSparse(int size, ByteBuf dataBuf, ServerSparseIntRow row);

  /**
   * Update int sparse data to int sparse row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateIntSparseToIntSparse(int size, ByteBuf dataBuf, ServerSparseIntRow row);

  /**
   * Update double dense to double dense.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleDenseToDoubleDense(int size, ByteBuf dataBuf, ServerDenseDoubleRow row);

  /**
   * Update double sparse to double dense.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleSparseToDoubleDense(int size, ByteBuf dataBuf, ServerDenseDoubleRow row);

  /**
   * Update double dense data to double sparse row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleDenseToDoubleSparse(int size, ByteBuf dataBuf, ServerSparseDoubleRow row);

  /**
   * Update double sparse data to double sparse row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleSparseToDoubleSparse(int size, ByteBuf dataBuf, ServerSparseDoubleRow row);

  /**
   * Update double sparse data with long key to double sparse  row with long key.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateDoubleSparseToDoubleSparseLongKey(int size, ByteBuf dataBuf, ServerSparseDoubleLongKeyRow row);

  /**
   * Update float dense data to float dense row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatDenseToFloatDense(int size, ByteBuf dataBuf, ServerDenseFloatRow row);

  /**
   * Update float sparse data to float dense row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatSparseToFloatDense(int size, ByteBuf dataBuf, ServerDenseFloatRow row);

  /**
   * Update float dense data to float sparse row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatDenseToFloatSparse(int size, ByteBuf dataBuf, ServerSparseFloatRow row);

  /**
   * Update float sparse data to float sparse row.
   *
   * @param size    the size
   * @param dataBuf the data buf
   * @param row     the row
   */
  public abstract void updateFloatSparseToFloatSparse(int size, ByteBuf dataBuf, ServerSparseFloatRow row);

  private void updateIntArbitrary(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerArbitraryIntRow row) {
    switch (updateRowType) {
      case T_INT_SPARSE:
        updateIntSparseToIntArbitrary(size, dataBuf, row);
        break;
      case T_INT_DENSE:
        updateIntDenseToIntArbitrary(size, dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateIntDense(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerDenseIntRow row) {
    switch (updateRowType) {
      case T_INT_SPARSE:
        updateIntSparseToIntDense(size, dataBuf, row);
        break;
      case T_INT_DENSE:
        updateIntDenseToIntDense(size, dataBuf, row);
        break;

      default:
        break;
    }
  }


  private void updateIntSparse(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerSparseIntRow row) {
    switch (updateRowType) {
      case T_INT_SPARSE:
        updateIntSparseToIntSparse(size, dataBuf, row);
        break;
      case T_INT_DENSE:
        updateIntDenseToIntSparse(size, dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateDoubleDense(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerDenseDoubleRow row) {
    switch (updateRowType) {
      case T_DOUBLE_SPARSE:
        updateDoubleSparseToDoubleDense(size, dataBuf, row);
        break;
      case T_DOUBLE_DENSE:
        updateDoubleDenseToDoubleDense(size, dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateDoubleSparse(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerSparseDoubleRow row) {
    switch (updateRowType) {
      case T_DOUBLE_SPARSE:
        updateDoubleSparseToDoubleSparse(size, dataBuf, row);
        break;
      case T_DOUBLE_DENSE:
        updateDoubleDenseToDoubleSparse(size, dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateDoubleSparseLongKey(RowType updateRowType, int size, ByteBuf dataBuf,
    ServerSparseDoubleLongKeyRow row) {
    switch (updateRowType) {
      case T_DOUBLE_SPARSE_LONGKEY:
        updateDoubleSparseToDoubleSparseLongKey(size, dataBuf, row);
        break;

      default:
        break;
    }
  }

  private void updateFloatDense(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerDenseFloatRow row) {
    switch (updateRowType) {
      case T_FLOAT_DENSE:
        updateFloatDenseToFloatDense(size, dataBuf, row);
        break;

      case T_FLOAT_SPARSE:
        updateFloatSparseToFloatDense(size, dataBuf, row);

      default:
        break;
    }
  }

  private void updateFloatSparse(RowType updateRowType, int size, ByteBuf dataBuf,
      ServerSparseFloatRow row) {
    switch (updateRowType) {
      case T_FLOAT_DENSE:
        updateFloatDenseToFloatSparse(size, dataBuf, row);
        break;
      case T_FLOAT_SPARSE:
        updateFloatSparseToFloatSparse(size, dataBuf, row);
        break;

      default:
        break;
    }
  }

}
