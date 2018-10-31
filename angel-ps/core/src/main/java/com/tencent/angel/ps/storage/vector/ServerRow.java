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


package com.tencent.angel.ps.storage.vector;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.math2.VFactory;
import com.tencent.angel.ml.math2.vector.IntKeyVector;
import com.tencent.angel.ml.math2.vector.IntVector;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.IndexType;
import com.tencent.angel.ps.server.data.request.InitFunc;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ServerRow is the storage unit at PS Server when using RowBlock as storage format. Each Row from
 * worker is split among multiple PS Servers. Therefore, we need startCol and endCol to clarify the
 * position of this ServerRow.
 */
public abstract class ServerRow implements Serialize {
  private static final Log LOG = LogFactory.getLog(ServerRow.class);
  protected int clock;
  protected int rowId;
  protected RowType rowType;
  protected long endCol;
  protected long startCol;
  protected long estElemNum;
  protected int size;
  protected int rowVersion;
  protected boolean useIntKey;
  /**
   * Serialize type, 0 dense, 1 sparse
   */
  protected int serializeType = -1;

  /**
   * Serialize key type, 0 int, 1 long
   */
  protected int serializeKeyType = -1;
  protected final ReentrantReadWriteLock lock;
  public static volatile transient int maxLockWaitTimeMs = 10000;
  public static volatile transient float sparseToDenseFactor = 0.2f;
  public static volatile transient boolean useAdaptiveStorage = true;

  protected Vector row;

  /**
   * Create a new Server row.
   *
   * @param rowId      the row id
   * @param rowType    row type
   * @param startCol   the start col
   * @param endCol     the end col
   * @param estElemNum the estimated element number, use for sparse vector
   */
  public ServerRow(int rowId, RowType rowType, long startCol, long endCol, long estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new Server row.
   *
   * @param rowId      the row id
   * @param rowType    row type
   * @param startCol   the start col
   * @param endCol     the end col
   * @param estElemNum the estimated element number, use for sparse vector
   */
  public ServerRow(int rowId, RowType rowType, long startCol, long endCol, long estElemNum,
    Vector row) {
    this.rowId = rowId;
    this.rowType = rowType;
    this.startCol = startCol;
    this.endCol = endCol;
    this.rowVersion = 0;
    this.estElemNum = estElemNum;
    this.lock = new ReentrantReadWriteLock();
    this.row = row;
    if (this.row == null) {
      this.row = initRow(startCol, endCol, estElemNum);
    }
  }

  /**
   * Create a empty server row
   */
  public ServerRow() {
    this(0, RowType.T_DOUBLE_DENSE, 0, 0, 0);
  }

  /**
   * Gets row id.
   *
   * @return the row id
   */
  public int getRowId() {
    return rowId;
  }

  /**
   * Is use dense storage
   *
   * @return true means use dense storage
   */
  public boolean isDense() {
    return row.isDense();
  }

  /**
   * Is use sparse storage
   *
   * @return true means use sparse storage
   */
  public boolean isSparse() {
    return row.isSparse();
  }

  /**
   * Is use sorted sparse storage
   *
   * @return true means use sorted sparse storage
   */
  public boolean isSorted() {
    return row.isSorted();
  }

  /**
   * Gets row type.
   *
   * @return the row type
   */
  public RowType getRowType() {
    return rowType;
  }

  /**
   * Get inner row
   *
   * @return inner row
   */
  public Vector getSplit() {
    return row;
  }

  /**
   * Set the inner row
   *
   * @param row the new inner row
   */
  public void setSplit(Vector row) {
    this.row = row;
  }

  /**
   * Update row version.
   */
  protected void updateRowVersion() {
    rowVersion++;
  }

  /**
   * Gets end col.
   *
   * @return the end col
   */
  public long getEndCol() {
    return endCol;
  }

  /**
   * Gets start col.
   *
   * @return the start col
   */
  public long getStartCol() {
    return startCol;
  }

  /**
   * Gets row version.
   *
   * @return the row version
   */
  public int getRowVersion() {
    return rowVersion;
  }

  /**
   * Gets row lock.
   *
   * @return the lock
   */
  public ReentrantReadWriteLock getLock() {
    return lock;
  }

  /**
   * Sets clock.
   *
   * @param clock the clock
   */
  public void setClock(int clock) {
    this.clock = clock;
  }

  /**
   * Get clock value
   *
   * @return clock value
   */
  public int getClock() {
    return clock;
  }

  /**
   * Reset the inner row
   */
  public void reset() {
    row.clear();
  }

  /**
   * Update row data
   *
   * @param rowType the row type
   * @param buf     the buf
   */
  public abstract void update(RowType rowType, ByteBuf buf, UpdateOp op);

  /**
   * Gets row size.
   *
   * @return the inner row size
   */
  public abstract int size();

  /**
   * Serialize the inner row
   *
   * @param buf the dest buffer
   */
  protected abstract void serializeRow(ByteBuf buf);

  /**
   * De-serialize the inner row
   *
   * @param buf the source buffer
   */
  protected abstract void deserializeRow(ByteBuf buf);

  /**
   * Get the buffer size for serialize the inner row
   *
   * @return the buffer size for serialize the inner row
   */
  protected abstract int getRowSpace();

  /**
   * Clone this ServerRow
   *
   * @return the cloned ServerRow
   */
  public abstract ServerRow clone();

  /**
   * Try to get write lock
   *
   * @param milliseconds maximum wait time in milliseconds
   */
  public void startWrite(long milliseconds) {
    boolean ret;
    try {
      ret = lock.writeLock().tryLock(milliseconds, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      throw new WaitLockTimeOutException(
        "wait write lock timeout " + StringUtils.stringifyException(e), milliseconds);
    }

    if (!ret) {
      throw new WaitLockTimeOutException("wait write timeout", milliseconds);
    }
  }

  /**
   * Try to get write lock
   */
  public void startWrite() {
    startWrite(maxLockWaitTimeMs);
  }

  /**
   * Try to get read lock
   */
  public void startRead() {
    startRead(maxLockWaitTimeMs);
  }

  /**
   * Try to get read lock
   *
   * @param milliseconds maximum wait time in milliseconds
   */
  public void startRead(long milliseconds) {
    boolean ret;
    try {
      ret = lock.readLock().tryLock(milliseconds, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      throw new WaitLockTimeOutException(
        "wait read lock timeout " + StringUtils.stringifyException(e), milliseconds);
    }

    if (!ret) {
      throw new WaitLockTimeOutException("wait read timeout", milliseconds);
    }
  }

  /**
   * Release write lock
   */
  public void endWrite() {
    lock.writeLock().unlock();
  }

  /**
   * Release read lock
   */
  public void endRead() {
    lock.readLock().unlock();
  }

  public abstract void indexGet(IndexType indexType, int indexSize, ByteBuf in, ByteBuf out,
    InitFunc func) throws IOException;

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //////// network io method, for model transform
  /////////////////////////////////////////////////////////////////////////////////////////////////
  @Override public void serialize(ByteBuf buf) {
    startRead();
    try {
      serializeHead(buf);
      serializeRow(buf);
    } finally {
      endRead();
    }
  }

  private void serializeHead(ByteBuf buf) {
    buf.writeInt(rowId);
    buf.writeInt(clock);
    buf.writeLong(startCol);
    buf.writeLong(endCol);
    buf.writeInt(rowVersion);
    buf.writeInt(size());
    buf.writeInt(isDense() ? 0 : 1);
    buf.writeInt(useIntKey ? 0 : 1);
  }

  @Override public void deserialize(ByteBuf buf) {
    startWrite();
    try {
      deserializeHead(buf);
      row = initRow(startCol, endCol, size);
      deserializeRow(buf);
    } finally {
      endWrite();
    }
  }

  private void deserializeHead(ByteBuf buf) {
    rowId = buf.readInt();
    clock = buf.readInt();
    startCol = buf.readLong();
    endCol = buf.readLong();
    rowVersion = buf.readInt();
    size = buf.readInt();
    serializeType = buf.readInt();
    serializeKeyType = buf.readInt();
  }

  protected boolean useDenseSerialize() {
    if (serializeType == -1) {
      serializeType = isDense() ? 0 : 1;
    }
    return serializeType == 0;
  }

  protected boolean useIntKeySerialize() {
    if (serializeKeyType == -1) {
      serializeKeyType = useIntKey ? 0 : 1;
    }
    return serializeKeyType == 0;
  }

  @Override public int bufferLen() {
    return 4 * 6 + 2 * 8 + getRowSpace();
  }

  @Override public String toString() {
    return "ServerRow [rowId=" + rowId + ", clock=" + clock + ", endCol=" + endCol + ", startCol="
      + startCol + ", rowVersion=" + rowVersion + "]";
  }

  protected boolean useIntKey() {
    return row instanceof IntVector;
  }

  private Vector initRow(long startCol, long endCol, long estElemNum) {
    Vector ret;
    switch (rowType) {
      case T_DOUBLE_SPARSE:
      case T_DOUBLE_SPARSE_COMPONENT:
        if (useAdaptiveStorage && estElemNum > sparseToDenseFactor * (endCol - startCol)) {
          ret = VFactory.denseDoubleVector((int) (endCol - startCol));
        } else {
          ret = VFactory.sparseDoubleVector((int) (endCol - startCol), (int) estElemNum);
        }
        break;

      case T_DOUBLE_DENSE:
      case T_DOUBLE_DENSE_COMPONENT:
        ret = VFactory.denseDoubleVector((int) (endCol - startCol));
        break;

      case T_FLOAT_SPARSE:
      case T_FLOAT_SPARSE_COMPONENT:
        if (useAdaptiveStorage && estElemNum > sparseToDenseFactor * (endCol - startCol)) {
          ret = VFactory.denseFloatVector((int) (endCol - startCol));
        } else {
          ret = VFactory.sparseFloatVector((int) (endCol - startCol), (int) estElemNum);
        }
        break;

      case T_FLOAT_DENSE:
      case T_FLOAT_DENSE_COMPONENT:
        ret = VFactory.denseFloatVector((int) (endCol - startCol));
        break;

      case T_INT_SPARSE:
      case T_INT_SPARSE_COMPONENT:
        if (useAdaptiveStorage && estElemNum > sparseToDenseFactor * (endCol - startCol)) {
          ret = VFactory.denseIntVector((int) (endCol - startCol));
        } else {
          ret = VFactory.sparseIntVector((int) (endCol - startCol), (int) estElemNum);
        }
        break;

      case T_INT_DENSE:
      case T_INT_DENSE_COMPONENT:
        ret = VFactory.denseIntVector((int) (endCol - startCol));
        break;

      case T_LONG_SPARSE:
      case T_LONG_SPARSE_COMPONENT:
        if (useAdaptiveStorage && estElemNum > sparseToDenseFactor * (endCol - startCol)) {
          ret = VFactory.denseLongVector((int) (endCol - startCol));
        } else {
          ret = VFactory.sparseLongVector((int) (endCol - startCol), (int) estElemNum);
        }
        break;

      case T_LONG_DENSE:
      case T_LONG_DENSE_COMPONENT:
        ret = VFactory.denseLongVector((int) (endCol - startCol));
        break;

      case T_DOUBLE_SPARSE_LONGKEY:
        if (endCol - startCol < Integer.MAX_VALUE) {
          if (estElemNum > sparseToDenseFactor * (endCol - startCol)) {
            ret = VFactory.denseDoubleVector((int) (endCol - startCol));
          } else {
            ret = VFactory.sparseDoubleVector((int) (endCol - startCol), (int) estElemNum);
          }
        } else {
          ret = VFactory.sparseLongKeyDoubleVector(endCol - startCol, (int) estElemNum);
        }
        break;

      case T_DOUBLE_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyDoubleVector(endCol - startCol, (int) estElemNum);
        break;

      case T_FLOAT_SPARSE_LONGKEY:
        if (endCol - startCol < Integer.MAX_VALUE) {
          if (estElemNum > sparseToDenseFactor * (endCol - startCol)) {
            ret = VFactory.denseFloatVector((int) (endCol - startCol));
          } else {
            ret = VFactory.sparseFloatVector((int) (endCol - startCol), (int) estElemNum);
          }
        } else {
          ret = VFactory.sparseLongKeyFloatVector(endCol - startCol, (int) estElemNum);
        }
        break;

      case T_FLOAT_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyFloatVector(endCol - startCol, (int) estElemNum);
        break;

      case T_INT_SPARSE_LONGKEY:
        if (endCol - startCol < Integer.MAX_VALUE) {
          if (estElemNum > sparseToDenseFactor * (endCol - startCol)) {
            ret = VFactory.denseIntVector((int) (endCol - startCol));
          } else {
            ret = VFactory.sparseIntVector((int) (endCol - startCol), (int) estElemNum);
          }
        } else {
          ret = VFactory.sparseLongKeyIntVector(endCol - startCol, (int) estElemNum);
        }
        break;

      case T_INT_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyIntVector(endCol - startCol, (int) estElemNum);
        break;

      case T_LONG_SPARSE_LONGKEY:
        if (endCol - startCol < Integer.MAX_VALUE) {
          if (estElemNum > sparseToDenseFactor * (endCol - startCol)) {
            ret = VFactory.denseLongVector((int) (endCol - startCol));
          } else {
            ret = VFactory.sparseLongVector((int) (endCol - startCol), (int) estElemNum);
          }
        } else {
          ret = VFactory.sparseLongKeyLongVector(endCol - startCol, (int) estElemNum);
        }
        break;

      case T_LONG_SPARSE_LONGKEY_COMPONENT:
        ret = VFactory.sparseLongKeyLongVector(endCol - startCol, (int) estElemNum);
        break;

      case T_DOUBLE_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseDoubleVector((int) (endCol - startCol));
        break;

      case T_FLOAT_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseFloatVector((int) (endCol - startCol));
        break;

      case T_INT_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseIntVector((int) (endCol - startCol));
        break;

      case T_LONG_DENSE_LONGKEY_COMPONENT:
        ret = VFactory.denseLongVector((int) (endCol - startCol));
        break;

      default:
        throw new UnsupportedOperationException(
          "can not support " + rowType + " type row for ServerIntDoubleRow");
    }
    useIntKey = ret instanceof IntKeyVector;
    return ret;
  }
}
