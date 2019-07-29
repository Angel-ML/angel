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


package com.tencent.angel.ps.storage.partition.storage;

import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Row-based matrix partition storage, it use a <row id, server row> map as inner storage
 */
public class SparseServerRowsStorage extends ServerRowsStorage {

  /**
   *  <row id, server row> map
   */
  private Map<Integer, ServerRow> data;

  public SparseServerRowsStorage(int rowIdOffset, int rowNum) {
    super(rowIdOffset);
    data = new HashMap<>(rowNum);
  }

  @Override
  public void init() {

  }

  @Override
  public ServerRow getRow(int index) {
    return data.get(index);
  }

  @Override
  public List<ServerRow> getRows(List<Integer> rowIds) {
    List<ServerRow> rows = new ArrayList<>(rowIds.size());
    for (int rowId : rowIds) {
      rows.add(getRow(rowId));
    }
    return rows;
  }

  @Override
  public void putRow(int index, ServerRow row) {
    data.put(index, row);
  }

  @Override
  public void putRows(List<Integer> rowIds, List<ServerRow> rows) {
    assert rowIds.size() == rows.size();
    int size = rowIds.size();
    for (int i = 0; i < size; i++) {
      putRow(rowIds.get(i), rows.get(i));
    }
  }

  @Override
  public int getRowNum() {
    return data.size();
  }

  @Override
  public boolean hasRow(int index) {
    return data.containsKey(index);
  }

  @Override
  public Iterator<Entry<Integer, ServerRow>> iterator() {
    return data.entrySet().iterator();
  }

  @Override
  public void reset() {
    for (Entry<Integer, ServerRow> entry : data.entrySet()) {
      if (entry.getValue() != null) {
        entry.getValue().reset();
      }
    }
  }

  @Override
  public long getElemNum() {
    long num = 0L;
    for(ServerRow row : data.values()) {
      num += row.size();
    }
    return num;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    // Map size
    output.writeInt(data.size());
    int writeIndex = output.writerIndex();

    // Actual write size
    output.writeInt(0);
    int writeRowNum = 0;

    // Rows data
    for(Entry<Integer, ServerRow> rowEntry : data.entrySet()) {
      if(rowEntry.getValue() != null) {
        // Row id
        output.writeInt(rowEntry.getKey());
        // Row type
        output.writeInt(rowEntry.getValue().getRowType().getNumber());
        // Row data
        rowEntry.getValue().serialize(output);
        writeRowNum++;

      }
    }

    output.setInt(writeIndex, writeRowNum);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    // Array size
    input.readInt();

    // Actual write row number
    int writeRowNum = input.readInt();
    data = new HashMap<>(writeRowNum);

    // Rows data
    for(int i = 0; i < writeRowNum; i++) {
      // Row id
      int index = input.readInt();
      // Create empty server row
      ServerRow row = ServerRowFactory.createEmptyServerRow(RowType.valueOf(input.readInt()));
      // Row data
      row.deserialize(input);
      data.put(index, row);
    }
  }

  @Override
  public int bufferLen() {
    int len = 0;
    len += super.bufferLen();
    len += 8;

    // Rows data
    for(Entry<Integer, ServerRow> rowEntry : data.entrySet()) {
      if(rowEntry.getValue() != null) {
        len += 8;
        len += rowEntry.getValue().bufferLen();
      }
    }

    return len;
  }
}
