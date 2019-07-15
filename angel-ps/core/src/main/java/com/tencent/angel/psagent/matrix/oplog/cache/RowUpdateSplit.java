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


package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Row update split
 */
public abstract class RowUpdateSplit implements Serialize {

  protected final static Log LOG = LogFactory.getLog(RowUpdateSplit.class);
  /**
   * split start position
   */
  protected final int start;

  /**
   * split end position
   */
  protected final int end;

  /**
   * row index
   */
  protected int rowId;

  /**
   * row type
   */
  protected volatile RowType rowType;

  /**
   * Split context
   */
  protected RowUpdateSplitContext splitContext;

  /**
   * Deserialize inner vector, it is initialized if the update split been deserialized
   */
  protected Vector vector;

  /**
   * Create a new RowUpdateSplit.
   *
   * @param rowIndex row index
   * @param rowType row type
   * @param start split start position
   * @param end split end position
   */
  public RowUpdateSplit(int rowIndex, RowType rowType, int start, int end) {
    this.rowId = rowIndex;
    this.rowType = rowType;
    this.start = start;
    this.end = end;
  }

  /**
   * Get the end position of row update split
   *
   * @return int the end position of row update split
   */
  public int getEnd() {
    return end;
  }

  /**
   * Get the start position of row update split
   *
   * @return int the start position of row update split
   */
  public int getStart() {
    return start;
  }

  /**
   * Get the row type of row update split
   *
   * @return RowType row type
   */
  public RowType getRowType() {
    return rowType;
  }

  /**
   * Get the row type of row update split
   *
   * @return RowType row type
   */
  public void setRowType(RowType rowType) {
    this.rowType = rowType;
  }

  /**
   * Get the row index that the split update to
   *
   * @return int the row index that the split update to
   */
  public int getRowId() {
    return rowId;
  }

  /**
   * Get the element number in split
   *
   * @return int the element number in split
   */
  public long size() {
    return end - start;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(rowId);
    buf.writeInt(rowType.getNumber());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    rowId = buf.readInt();
    rowType = RowType.valueOf(buf.readInt());
  }

  @Override
  public int bufferLen() {
    return 8;
  }

  /**
   * Get row update split context
   *
   * @return row update split context
   */
  public RowUpdateSplitContext getSplitContext() {
    return splitContext;
  }

  /**
   * Set row update split context
   *
   * @param splitContext row update split context
   */
  public void setSplitContext(RowUpdateSplitContext splitContext) {
    this.splitContext = splitContext;
  }

  /**
   * Set row id
   *
   * @param rowId row id
   */
  public void setRowId(int rowId) {
    this.rowId = rowId;
  }

  /**
   * Get inner vector
   *
   * @return inner vector
   */
  public Vector getVector() {
    return vector;
  }

  /**
   * Is use int key in serialization
   *
   * @return true means use int key in serialization
   */
  public boolean isUseIntKey() {
    return true;
  }

  /**
   * Set use the int key in serialization
   *
   * @param useIntKey true means use int key in serialization
   */
  public void setUseIntKey(boolean useIntKey) {

  }
}
