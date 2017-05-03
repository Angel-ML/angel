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

package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.common.Serialize;
import com.tencent.angel.protobuf.generated.MLProtos.RowType;
import io.netty.buffer.ByteBuf;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * Row update split
 */
public abstract class RowUpdateSplit implements Serialize {
  protected final static Log LOG = LogFactory.getLog(RowUpdateSplit.class);
  /**split start position*/
  protected final int start;
  
  /**split end position*/
  protected final int end;
  
  /**row index*/
  protected final int rowId;
  
  /**row type*/
  protected final RowType rowType;

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
  public int size() {
    return end - start;
  }

  @Override
  public void serialize(ByteBuf buf) {
    buf.writeInt(rowId);
    buf.writeInt(rowType.getNumber());
  }

  @Override
  public void deserialize(ByteBuf buf) {
    //unused now
  }

  @Override
  public int bufferLen() {
    return 8;
  }
}
