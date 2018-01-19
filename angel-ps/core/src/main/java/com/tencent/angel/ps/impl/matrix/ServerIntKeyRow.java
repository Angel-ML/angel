package com.tencent.angel.ps.impl.matrix;

import io.netty.buffer.ByteBuf;

public abstract class ServerIntKeyRow extends ServerRow {
  /**
   * Create a new ServerDoubleRow row.
   *
   * @param rowId    the row id
   * @param startCol the start col
   * @param endCol   the end col
   */
  public ServerIntKeyRow(int rowId, long startCol, long endCol) {
    super(rowId, startCol, endCol);
  }

  /**
   * Create a new ServerDoubleRow row, just for Serialize/Deserialize
   */
  public ServerIntKeyRow() {
    this(0, 0, 0);
  }

  /**
   * Batch get values use indexes, write the values to buffer
   * @param indexes
   * @param buffer result buffer
   */
  public abstract void getValues(int [] indexes, ByteBuf buffer);
}
