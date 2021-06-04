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

import com.tencent.angel.common.ByteBufSerdeUtils;
import com.tencent.angel.ps.storage.vector.ServerRow;
import io.netty.buffer.ByteBuf;

/**
 * The result of get batch row splits rpc request.
 */
public class GetRowsSplitResponse extends ResponseData {
  /**
   * row splits
   */
  private ServerRow[] rowsSplit;

  /**
   * Create a new GetRowsSplitResponse.
   *
   * @param rowsSplit    row splits
   */
  public GetRowsSplitResponse(ServerRow[] rowsSplit) {
    this.setRowsSplit(rowsSplit);
  }

  /**
   * Create a new GetRowsSplitResponse.
   */
  public GetRowsSplitResponse() {
    this(null);
  }

  /**
   * Get the row splits.
   *
   * @return List<ServerRow> row splits
   */
  public ServerRow[] getRowsSplit() {
    return rowsSplit;
  }

  /**
   * Set the row splits.
   *
   * @param rowsSplit row splits
   */
  public void setRowsSplit(ServerRow[] rowsSplit) {
    this.rowsSplit = rowsSplit;
  }

  @Override public void serialize(ByteBuf buf) {
    ByteBufSerdeUtils.serializeInt(buf, rowsSplit.length);
    for(int i = 0; i < rowsSplit.length; i++) {
      ByteBufSerdeUtils.serializeServerRow(buf, rowsSplit[i]);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    rowsSplit = new ServerRow[ByteBufSerdeUtils.deserializeInt(buf)];
    for(int i = 0; i < rowsSplit.length; i++) {
      rowsSplit[i] = ByteBufSerdeUtils.deserializeServerRow(buf);
    }
  }

  @Override public int bufferLen() {
    int len = ByteBufSerdeUtils.INT_LENGTH;
    for(int i = 0; i < rowsSplit.length; i++) {
      len += ByteBufSerdeUtils.serializedServerRowLen(rowsSplit[i]);
    }
    return len;
  }
}
