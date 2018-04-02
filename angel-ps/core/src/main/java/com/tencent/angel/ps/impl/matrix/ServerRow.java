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

import com.tencent.angel.common.Serialize;
import com.tencent.angel.exception.WaitLockTimeOutException;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.utils.StringUtils;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * ServerRow is the storage unit at PS Server when using RowBlock as storage format. Each Row from
 * worker is split among multiple PS Servers. Therefore, we need startCol and endCol to clarify the
 * position of this ServerRow.
 *
 */
public abstract class ServerRow implements Serialize {
  private static final Log LOG = LogFactory.getLog(ServerRow.class);
  protected int clock;
  protected int rowId;
  protected long endCol;
  protected long startCol;
  protected int rowVersion;
  protected final ReentrantReadWriteLock lock;
  public static volatile transient int maxLockWaitTimeMs = 10000;

  /**
   * Create a new Server row.
   *
   * @param rowId    the row id
   * @param startCol the start col
   * @param endCol   the end col
   */
  public ServerRow(int rowId, long startCol, long endCol) {
    this.rowId = rowId;
    this.startCol = startCol;
    this.endCol = endCol;
    this.rowVersion = 0;
    this.lock = new ReentrantReadWriteLock();
  }

  public ServerRow() {
    this(0, 0, 0);
  }

  /**
   * Gets row type.
   *
   * @return the row type
   */
  public abstract RowType getRowType();

  /**
   * Update row data
   *
   * @param rowType the row type
   * @param buf     the buf
   */
  public abstract void update(RowType rowType, ByteBuf buf);

  /**
   * Try to get write lock
   * @param milliseconds maximum wait time in milliseconds
   */
  public void tryToLockWrite(long milliseconds) {
    boolean ret;
    try {
      ret = lock.writeLock().tryLock(milliseconds, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      throw new WaitLockTimeOutException("wait write lock timeout " + StringUtils.stringifyException(e), milliseconds);
    }

    if(!ret) {
      throw new WaitLockTimeOutException("wait write timeout", milliseconds);
    }
  }

  /**
   * Try to get write lock
   */
  public void tryToLockWrite() {
    tryToLockWrite(maxLockWaitTimeMs);
  }

  /**
   * Try to get read lock
   */
  public void tryToLockRead() {
    tryToLockRead(maxLockWaitTimeMs);
  }

  /**
   * Try to get read lock
   * @param milliseconds maximum wait time in milliseconds
   */
  public void tryToLockRead(long milliseconds){
    boolean ret;
    try {
      ret = lock.readLock().tryLock(milliseconds, TimeUnit.MILLISECONDS);
    } catch (Throwable e) {
      throw new WaitLockTimeOutException("wait read lock timeout " + StringUtils.stringifyException(e), milliseconds);
    }

    if(!ret) {
      throw new WaitLockTimeOutException("wait read timeout", milliseconds);
    }
  }

  /**
   * Get write lock
   */
  public void lockWrite() {
    lock.writeLock().lock();
  }

  /**
   * Get read lock
   */
  public void lockRead() {
    lock.readLock().lock();
  }

  /**
   * Release write lock
   */
  public void unlockWrite(){
    lock.writeLock().unlock();
  }

  /**
   * Release read lock
   */
  public void unlockRead(){
    lock.readLock().unlock();
  }

  /**
   * Write row to output
   *
   * @param output the output
   * @throws IOException
   */
  protected void writeTo(DataOutputStream output) throws IOException{
    //output.writeInt(clock);
  }

  /**
   * Read row from input
   *
   * @param input the input
   * @throws IOException
   */
  protected void readFrom(DataInputStream input) throws IOException{
    //clock = input.readInt();
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(rowId);
    buf.writeInt(clock);
    buf.writeLong(startCol);
    buf.writeLong(endCol);
    buf.writeInt(rowVersion);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    rowId = buf.readInt();
    clock = buf.readInt();
    startCol = buf.readLong();
    endCol = buf.readLong();
    rowVersion = buf.readInt();
  }

  /**
   * Gets row size.
   *
   * @return the size
   */
  public abstract int size();

  @Override
  public int bufferLen() {
    return 3 * 4 + 2 * 8;
  }

  /**
   * Update row version.
   */
  protected void updateRowVersion() {
    rowVersion++;
  }

  /**
   * Encode row.
   *
   * @param in  the in
   * @param out the out
   * @param len the len
   */
  public void encode(ByteBuf in, ByteBuf out, int len) {

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
   * Gets clock.
   *
   * @return the clock
   */
  public int getClock() {
    return clock;
  }


  /**
   * Sets row version.
   *
   * @param rowVersion the row version
   */
  public void setRowVersion(int rowVersion) {
    this.rowVersion = rowVersion;
  }

  /**
   * Sets start col.
   *
   * @param startCol the start col
   */
  public void setStartCol(int startCol) {
    this.startCol = startCol;
  }

  /**
   * Sets end col.
   *
   * @param endCol the end col
   */
  public void setEndCol(int endCol) {
    this.endCol = endCol;
  }

  /**
   * Sets clock.
   *
   * @param clock the clock
   */
  public void setClock(int clock) {
    this.clock = clock;
  }

  @Override
  public String toString() {
    return "ServerRow [rowId=" + rowId + ", clock=" + clock + ", endCol=" + endCol + ", startCol="
        + startCol + ", rowVersion=" + rowVersion + "]";
  }

  public abstract void reset();
}
