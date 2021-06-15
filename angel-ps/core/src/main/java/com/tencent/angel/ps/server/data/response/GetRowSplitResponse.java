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
 * The result of get row split rpc request.
 */
public class GetRowSplitResponse extends ResponseData {
  /**
   * row split
   */
  private ServerRow rowSplit;

  /**
   * Create a new GetRowSplitResponse.
   *
   * @param rowSplit     row split
   */
  public GetRowSplitResponse(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }

  /**
   * Create a new GetRowSplitResponse.
   */
  public GetRowSplitResponse() {
    this(null);
  }

  @Override public void serialize(ByteBuf buf) {
    ByteBufSerdeUtils.serializeServerRow(buf, rowSplit);
  }

  @Override public void deserialize(ByteBuf buf) {
    rowSplit = ByteBufSerdeUtils.deserializeServerRow(buf);
  }

  @Override public int bufferLen() {
    return ByteBufSerdeUtils.serializedServerRowLen(rowSplit);
  }

  /**
   * Get row split.
   *
   * @return ServerRow row split.
   */
  public ServerRow getRowSplit() {
    return rowSplit;
  }

  /**
   * Set row split.
   *
   * @param rowSplit row split
   */
  public void setRowSplit(ServerRow rowSplit) {
    this.rowSplit = rowSplit;
  }
}
