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
import com.tencent.angel.common.StreamSerialize;
import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.server.data.request.UpdateOp;
import com.tencent.angel.ps.storage.vector.op.GeneralOp;
import com.tencent.angel.ps.storage.vector.storage.IStorage;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * ServerRow is the storage unit at PS Server when using RowBlock as storage format. Each Row from
 * worker is split among multiple PS Servers. Therefore, we need startCol and endCol to clarify the
 * position of this ServerRow.
 */
public abstract class ServerRow implements GeneralOp {

  private static final Log LOG = LogFactory.getLog(ServerRow.class);
  protected int clock;
  protected int rowId;
  protected RowType rowType;
  protected long endCol;
  protected long startCol;
  protected long estElemNum;
  protected int size;
  protected int rowVersion;

  protected final ReentrantReadWriteLock lock;
  public static volatile transient int maxLockWaitTimeMs = 10000;
  public static volatile transient float sparseToDenseFactor = 0.2f;
  public static volatile transient boolean useAdaptiveKey = true;
  public static volatile transient boolean useAdaptiveStorage = true;

  /**
   * Row element storage
   */
  protected IStorage storage;

  /**
   * Create a new Server row.
   *
   * @param rowId the row id
   * @param rowType row type
   * @param startCol the start col
   * @param endCol the end col
   * @param estElemNum the estimated element number, use for sparse vector
   */
  public ServerRow(int rowId, RowType rowType, long startCol, long endCol, long estElemNum) {
    this(rowId, rowType, startCol, endCol, estElemNum, null);
  }

  /**
   * Create a new Server row.
   *
   * @param rowId the row id
   * @param rowType row type
   * @param startCol the start col
   * @param endCol the end col
   * @param estElemNum the estimated element number, use for sparse vector
   */
  public ServerRow(int rowId, RowType rowType, long startCol, long endCol, long estElemNum,
      IStorage storage) {
    this.rowId = rowId;
    this.rowType = rowType;
    this.startCol = startCol;
    this.endCol = endCol;
    this.rowVersion = 0;
    this.estElemNum = estElemNum;
    this.lock = new ReentrantReadWriteLock();
    this.storage = storage;
  }

  public void init() {
    if (storage == null) {
      initStorage();
    }
  }

  protected abstract void initStorage();

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
    return storage.isDense();
  }

  /**
   * Is use sparse storage
   *
   * @return true means use sparse storage
   */
  public boolean isSparse() {
    return storage.isSparse();
  }

  /**
   * Is use sorted sparse storage
   *
   * @return true means use sorted sparse storage
   */
  public boolean isSorted() {
    return storage.isSorted();
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
    storage.clear();
  }

  @Override
  public void clear() {
    storage.clear();
  }

  public IStorage getStorage() {
    return storage;
  }

  @Override
  public int size() {
    return storage.size();
  }

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


  @Override
  public void update(RowType updateType, ByteBuf buf, UpdateOp op) {
    startWrite();
    try {
      getStorage().update(updateType, buf, op);
      updateRowVersion();
    } finally {
      endWrite();
    }
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  //////// network io method, for model transform
  /////////////////////////////////////////////////////////////////////////////////////////////////
  @Override
  public void serialize(ByteBuf buf) {
    startRead();
    try {
      // Serailize the head
      buf.writeInt(rowId);
      buf.writeInt(rowType.getNumber());
      buf.writeInt(clock);
      buf.writeLong(startCol);
      buf.writeLong(endCol);
      buf.writeInt(rowVersion);

      // Serialize the storage
      byte[] data = storage.getClass().getName().getBytes();
      buf.writeInt(data.length);
      buf.writeBytes(data);
      storage.serialize(buf);
    } finally {
      endRead();
    }
  }


  @Override
  public void deserialize(ByteBuf buf) {
    startWrite();
    try {
      // Deserailze the head
      rowId = buf.readInt();
      rowType = RowType.valueOf(buf.readInt());
      clock = buf.readInt();
      startCol = buf.readLong();
      endCol = buf.readLong();
      rowVersion = buf.readInt();

      // Deseralize the storage
      int size = buf.readInt();
      byte[] data = new byte[size];
      buf.readBytes(data);
      String storageClassName = new String(data);

      storage = ServerRowStorageFactory.getStorage(storageClassName);
      storage.deserialize(buf);
    } finally {
      endWrite();
    }
  }

  @Override
  public int bufferLen() {
    int headLen = 4 * 4 + 2 * 8 + 4 + storage.getClass().getName().getBytes().length;
    return headLen + storage.bufferLen();
  }

  @Override
  public void serialize(DataOutputStream out) throws IOException {
    startRead();
    try {
      // Serailize the head
      out.writeInt(rowId);
      out.writeInt(rowType.getNumber());
      out.writeInt(clock);
      out.writeLong(startCol);
      out.writeLong(endCol);
      out.writeInt(rowVersion);

      // Serialize the storage
      byte[] data = storage.getClass().getName().getBytes();
      out.writeInt(data.length);
      out.write(data);
      storage.serialize(out);
    } finally {
      endRead();
    }
  }


  @Override
  public void deserialize(DataInputStream in) throws IOException {
    startWrite();
    try {
      // Deserailze the head
      rowId = in.readInt();
      rowType = RowType.valueOf(in.readInt());
      clock = in.readInt();
      startCol = in.readLong();
      endCol = in.readLong();
      rowVersion = in.readInt();

      // Deseralize the storage
      int size = in.readInt();
      byte[] data = new byte[size];
      in.readFully(data, 0, data.length);
      String storageClassName = new String(data);

      storage = ServerRowStorageFactory.getStorage(storageClassName);
      storage.deserialize(in);
    } finally {
      endWrite();
    }
  }

  @Override
  public int dataLen() {
    return bufferLen();
  }

  @Override
  public String toString() {
    return "ServerRow [rowId=" + rowId + ", clock=" + clock + ", endCol=" + endCol + ", startCol="
        + startCol + ", rowVersion=" + rowVersion + "]";
  }
}
