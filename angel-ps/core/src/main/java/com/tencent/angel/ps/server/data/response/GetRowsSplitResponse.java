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


package com.tencent.angel.ps.server.data.response;

import com.tencent.angel.ml.math2.utils.RowType;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import io.netty.buffer.ByteBuf;

import java.util.ArrayList;
import java.util.List;

/**
 * The result of get batch row splits rpc request.
 */
public class GetRowsSplitResponse extends Response {
  /**
   * row splits
   */
  private List<ServerRow> rowsSplit;

  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param responseType response type
   * @param detail       detail error message if the response type is error
   * @param rowsSplit    row splits
   */
  public GetRowsSplitResponse(ResponseType responseType, String detail, List<ServerRow> rowsSplit) {
    super(responseType, detail);
    this.setRowsSplit(rowsSplit);
  }

  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param responseType response type
   * @param rowsSplit    row splits
   */
  public GetRowsSplitResponse(ResponseType responseType, List<ServerRow> rowsSplit) {
    this(responseType, null, rowsSplit);
  }


  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param responseType response type
   * @param detail       detail error message if the response type is error
   */
  public GetRowsSplitResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }

  /**
   * Create a new GetRowsSplitResponse.
   */
  public GetRowsSplitResponse() {
    this(ResponseType.SUCCESS, null, null);
  }

  /**
   * Get the row splits.
   *
   * @return List<ServerRow> row splits
   */
  public List<ServerRow> getRowsSplit() {
    return rowsSplit;
  }

  /**
   * Set the row splits.
   *
   * @param rowsSplit row splits
   */
  public void setRowsSplit(List<ServerRow> rowsSplit) {
    this.rowsSplit = rowsSplit;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (rowsSplit != null) {
      int size = rowsSplit.size();
      buf.writeInt(size);
      for (int i = 0; i < size; i++) {
        buf.writeInt(rowsSplit.get(i).getRowType().getNumber());
        rowsSplit.get(i).serialize(buf);
      }
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.readableBytes() == 0) {
      rowsSplit = null;
      return;
    }

    int size = buf.readInt();
    rowsSplit = new ArrayList<>();
    for (int i = 0; i < size; i++) {
      ServerRow rowSplit = ServerRowFactory.createEmptyServerRow(RowType.valueOf(buf.readInt()));
      rowSplit.deserialize(buf);
      rowsSplit.add(rowSplit);
    }
  }

  @Override public int bufferLen() {
    int len = super.bufferLen();
    if (rowsSplit != null) {
      int size = rowsSplit.size();
      for (int i = 0; i < size; i++) {
        len += rowsSplit.get(i).bufferLen();
      }
    }
    return len;
  }

  @Override public void clear() {
    setRowsSplit(null);
  }
}
