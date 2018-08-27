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


package com.tencent.angel.psagent.matrix.transport.adapter;

import com.tencent.angel.ps.server.data.request.UpdateItem;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplit;
import io.netty.buffer.ByteBuf;

import java.util.List;

public class RowSplitsUpdateItem extends UpdateItem {
  private final List<RowUpdateSplit> rowSplits;

  public RowSplitsUpdateItem(List<RowUpdateSplit> rowSplits) {
    this.rowSplits = rowSplits;
  }

  public RowSplitsUpdateItem() {
    this(null);
  }

  @Override public int size() {
    int size = 0;
    for (RowUpdateSplit split : rowSplits) {
      size += split.size();
    }
    return size;
  }

  @Override public void serialize(ByteBuf buf) {
    int size = rowSplits.size();
    buf.writeInt(size);
    for (int i = 0; i < size; i++) {
      rowSplits.get(i).serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {

  }

  @Override public int bufferLen() {
    int len = 4;
    for (RowUpdateSplit split : rowSplits) {
      len += split.bufferLen();
    }
    return len;
  }
}
