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

import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Row-based matrix partition storage, it use a server row array as inner storage
 */
public class DenseServerRowsStorage extends ServerRowsStorage {

  /**
   * Server rows
   */
  private ServerRow[] data;

  /**
   * Create new DenseServerRowsStorage
   *
   * @param rowIdOffset row id offset
   * @param rowNum row number
   */
  public DenseServerRowsStorage(int rowIdOffset, int rowNum) {
    super(rowIdOffset);
    this.data = new ServerRow[rowNum];
  }

  @Override
  public ServerRow getRow(int index) {
    return data[index - rowIdOffset];
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
    data[index - rowIdOffset] = row;
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
    return data.length;
  }

  @Override
  public boolean hasRow(int index) {
    return (index - rowIdOffset) >= 0 && (index - rowIdOffset) < data.length;
  }

  @Override
  public Iterator<Entry<Integer, ServerRow>> iterator() {

    return new Iterator<Map.Entry<Integer, ServerRow>>() {
      int currIndex = 0;

      @Override
      public boolean hasNext() {
        return currIndex < data.length;
      }

      @Override
      public Map.Entry<Integer, ServerRow> next() {
        Map.Entry<Integer, ServerRow> entry = new AbstractMap.SimpleEntry(currIndex + rowIdOffset,
            data[currIndex]);
        currIndex++;
        return entry;
      }
    };
  }

  @Override
  public void init() {

  }

  @Override
  public void reset() {
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        data[i].reset();
      }
    }
  }

  @Override
  public long getElemNum() {
    long ret = 0L;
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        ret += data[i].size();
      }
    }
    return ret;
  }

  @Override
  public void serialize(ByteBuf output) {
    super.serialize(output);
    // Array size
    output.writeInt(data.length);
    int writeIndex = output.writerIndex();

    // Actual write size
    output.writeInt(0);
    int writeRowNum = 0;

    // Rows data
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        // Row id
        output.writeInt(i);
        // Row type
        output.writeInt(data[i].getRowType().getNumber());
        // Row data
        data[i].serialize(output);
        writeRowNum++;
      }
    }

    output.setInt(writeIndex, writeRowNum);
  }

  @Override
  public void deserialize(ByteBuf input) {
    super.deserialize(input);
    // Array size
    int len = input.readInt();
    data = new ServerRow[len];

    // Actual write row number
    int writeRowNum = input.readInt();

    // Rows data
    for (int i = 0; i < writeRowNum; i++) {
      // Row id
      int index = input.readInt();
      // Create empty server row
      data[index] = ServerRowFactory.createEmptyServerRow(RowType.valueOf(input.readInt()));
      // Row data
      data[index].deserialize(input);
    }
  }

  @Override
  public int bufferLen() {
    int len = 0;
    len += super.bufferLen();
    len += 8;

    // Rows data
    for (int i = 0; i < data.length; i++) {
      if (data[i] != null) {
        len += 8;
        len += data[i].bufferLen();
      }
    }
    return len;
  }
}
