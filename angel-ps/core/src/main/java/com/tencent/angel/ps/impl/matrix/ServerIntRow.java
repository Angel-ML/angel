package com.tencent.angel.ps.impl.matrix;

import io.netty.buffer.ByteBuf;

public abstract class ServerIntRow extends ServerIntKeyRow {
  /**
   * Create a new ServerDoubleRow row.
   *
   * @param rowId    the row id
   * @param startCol the start col
   * @param endCol   the end col
   */
  public ServerIntRow(int rowId, long startCol, long endCol) {
    super(rowId, startCol, endCol);
  }

  /**
   * Create a new ServerDoubleRow row, just for Serialize/Deserialize
   */
  public ServerIntRow() {
    this(0, 0, 0);
  }

  /**
   * Batch get values use indexes
   * @param indexes elements indexes
   * @return element values
   */
  public int[] getValues(int [] indexes) {
    int [] values = new int[indexes.length];
    try {
      lock.readLock().lock();
      int len = indexes.length;
      for(int i = 0; i < len; i++) {
        values[i] = getValue(indexes[i]);
      }
      return values;
    } finally {
      lock.readLock().unlock();
    }
  }

  @Override
  public void getValues(int [] indexes, ByteBuf buffer) {
    try {
      lock.readLock().lock();
      int len = indexes.length;
      for(int i = 0; i < len; i++) {
        buffer.writeInt(getValue(indexes[i]));
      }
    } finally {
      lock.readLock().unlock();
    }
  }

  /**
   * Get element value use index
   * @param index element index
   * @return element value of index
   */
  protected abstract int getValue(int index);
}
