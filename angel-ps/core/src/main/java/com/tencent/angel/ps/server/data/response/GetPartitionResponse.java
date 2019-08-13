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

import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.partition.ServerPartitionFactory;
import io.netty.buffer.ByteBuf;

/**
 * The result of get matrix partition rpc.
 */
public class GetPartitionResponse extends Response {
  /**
   * matrix partition
   */
  private ServerPartition partition;

  /**
   * Create a new GetPartitionResponse.
   *
   * @param responseType response type
   * @param detail       the detail information of response
   * @param partition    matrix partition
   */
  public GetPartitionResponse(ResponseType responseType, String detail, ServerPartition partition) {
    super(responseType, detail);
    this.partition = partition;
  }

  /**
   * Create a new GetPartitionResponse.
   *
   * @param responseType response type
   * @param detail       the detail information of response
   */
  public GetPartitionResponse(ResponseType responseType, String detail) {
    this(responseType, detail, null);
  }

  /**
   * Create a new GetPartitionResponse.
   *
   * @param responseType response type
   * @param partition    matrix partition
   */
  public GetPartitionResponse(ResponseType responseType, ServerPartition partition) {
    this(responseType, null, partition);
  }

  /**
   * Create a new GetPartitionResponse.
   */
  public GetPartitionResponse() {
    this(ResponseType.SUCCESS, null, null);
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (partition != null) {
      byte []  partClass = partition.getClass().getName().getBytes();
      buf.writeInt(partClass.length);
      buf.writeBytes(partClass);
      partition.serialize(buf);
    }
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.readableBytes() == 0) {
      partition = null;
      return;
    }

    byte [] partClass = new byte[buf.readInt()];
    buf.readBytes(partClass);
    String partClassName = new String(partClass);

    partition = ServerPartitionFactory.getPartition(partClassName);
    partition.deserialize(buf);
  }

  @Override public int bufferLen() {
    return super.bufferLen() + (partition != null ? partition.bufferLen() : 0);
  }

  /**
   * Get partition
   *
   * @return partition
   */
  public ServerPartition getPartition() {
    return partition;
  }

  /**
   * Set partitoin
   *
   * @param partition partition
   */
  public void setPartition(ServerPartition partition) {
    this.partition = partition;
  }

}
