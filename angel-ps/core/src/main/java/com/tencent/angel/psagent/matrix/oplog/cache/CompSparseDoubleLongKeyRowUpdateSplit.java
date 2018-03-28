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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.psagent.matrix.oplog.cache;

import com.tencent.angel.ml.math.vector.SparseLongKeyDoubleVector;
import com.tencent.angel.ml.matrix.RowType;
import io.netty.buffer.ByteBuf;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

/**
 * Row split of component sparse double long key row update.
 */
public class CompSparseDoubleLongKeyRowUpdateSplit extends RowUpdateSplit {

  private final SparseLongKeyDoubleVector split;

  /**
   * Create a new CompSparseDoubleLongKeyRowUpdateSplit
   *
   * @param rowIndex row index
   * @param rowType  row type
   */
  public CompSparseDoubleLongKeyRowUpdateSplit(SparseLongKeyDoubleVector split, int rowIndex,
                                               RowType rowType) {
    super(rowIndex, rowType, -1, -1);
    this.split = split;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeDouble(split.getIndexToValueMap().defaultReturnValue());
    buf.writeInt(split.size());

    ObjectIterator<Long2DoubleMap.Entry> iter =
      split.getIndexToValueMap().long2DoubleEntrySet().fastIterator();
    Long2DoubleMap.Entry entry = null;
    while (iter.hasNext()) {
      entry = iter.next();
      buf.writeLong(entry.getLongKey());
      buf.writeDouble(entry.getDoubleValue());
    }
  }

  @Override public long size() {
    return split.size();
  }

  @Override public int bufferLen() {
    return 8 + 4 + super.bufferLen() + split.size() * 16;
  }
}
