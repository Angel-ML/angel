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

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.server.data.request.ValueType;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class IndexPartGetRowsLongResult extends IndexPartGetRowsResult {
  private volatile Map<Integer, long[]> values;

  public IndexPartGetRowsLongResult(PartitionKey partKey, List<Integer> rowIds, IndicesView indices,
    Map<Integer, long[]> values) {
    super(partKey, rowIds, indices);
    this.values = values;
  }

  public IndexPartGetRowsLongResult() {
    this(null, null, null, null);
  }

  @Override public void serializeData(ByteBuf buf) {
    buf.writeInt(values.size());
    int len = 0;
    for (Map.Entry<Integer, long[]> entry : values.entrySet()) {
      len = entry.getValue().length;
      break;
    }
    for (Map.Entry<Integer, long[]> entry : values.entrySet()) {
      buf.writeInt(entry.getKey());
      long[] values = entry.getValue();
      for (int i = 0; i < len; i++) {
        buf.writeLong(values[i]);
      }
    }
  }

  @Override public void deserializeData(ByteBuf buf) {
    int rowNum = buf.readInt();
    int colNum = buf.readInt();
    values = new HashMap<>(rowNum);

    for (int i = 0; i < rowNum; i++) {
      int rowId = buf.readInt();
      long[] colVals = new long[colNum];
      for (int j = 0; j < colNum; j++) {
        colVals[j] = buf.readLong();
      }
      values.put(rowId, colVals);
    }
  }

  @Override public int getDataSize() {
    return 4 + 4 + values.size() * (4 + values.values().iterator().next().length * 8);
  }

  @Override public ValueType getValueType() {
    return ValueType.LONG;
  }

  public Map<Integer, long[]> getValues() {
    return values;
  }
}
