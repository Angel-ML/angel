package com.tencent.angel.ps.impl.matrix;

import io.netty.buffer.ByteBuf;

/**
 * Base class for double row split
 */
public abstract class ServerDoubleRow extends ServerIntKeyRow {
  /**
   * Create a new ServerDoubleRow row.
   *
   * @param rowId    the row id
   * @param startCol the start col
   * @param endCol   the end col
   */
  public ServerDoubleRow(int rowId, long startCol, long endCol) {
    super(rowId, startCol, endCol);
  }

  /**
   * Create a new ServerDoubleRow row, just for Serialize/Deserialize
   */
  public ServerDoubleRow() {
    this(0, 0, 0);
  }

  /**
   * Batch get values use indexes
   * @param indexes elements indexes
   * @return element values
   */
  public double[] getValues(int[] indexes) {
    double [] values = new double[indexes.length];
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
  public void getValues(int[] indexes, ByteBuf buffer) {
    try {
      lock.readLock().lock();
      int len = indexes.length;
      for(int i = 0; i < len; i++) {
        buffer.writeDouble(getValue(indexes[i]));
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
  protected abstract double getValue(int index);
}
