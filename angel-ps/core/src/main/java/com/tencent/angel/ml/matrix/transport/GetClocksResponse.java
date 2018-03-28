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
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import io.netty.buffer.ByteBuf;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

/**
 * The result of the get clock rpc request.
 */
public class GetClocksResponse extends Response {
  /** matrix partition to clock value map */
  private Map<PartitionKey, Integer> clocks;

  /**
   * Create a new GetClocksResponse.
   *
   * @param responseType response type
   * @param detail detail response information
   * @param clocks clock values of matrix partitions
   */
  public GetClocksResponse(ResponseType responseType, String detail,
      Map<PartitionKey, Integer> clocks) {
    super(responseType, detail);
    this.setClocks(clocks);
  }

  /**
   * Create a new GetClocksResponse.
   */
  public GetClocksResponse() {
    super();
    clocks = null;
  }

  /**
   * Get the clocks of matrix partitions.
   * 
   * @return Map<PartitionKey, Integer> the clocks of matrix partitions
   */
  public Map<PartitionKey, Integer> getClocks() {
    return clocks;
  }

  /**
   * Set the clocks of matrix partitions.
   * 
   * @param clocks the clocks of matrix partitions
   */
  public void setClocks(Map<PartitionKey, Integer> clocks) {
    this.clocks = clocks;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    if (clocks != null) {
      buf.writeInt(clocks.size());
      for (Entry<PartitionKey, Integer> entry : clocks.entrySet()) {
        entry.getKey().serialize(buf);
        buf.writeInt(entry.getValue());
      }
    }
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    if (buf.readableBytes() != 0) {
      int size = buf.readInt();
      clocks = new HashMap<>(size);
      for (int i = 0; i < size; i++) {
        PartitionKey partKey = new PartitionKey();
        partKey.deserialize(buf);
        clocks.put(partKey, buf.readInt());
      }
    }
  }

  @Override
  public int bufferLen() {
    int len = super.bufferLen();
    if(clocks != null) {
      len += (4 + clocks.size() * 32);
    }
    return len;
  }

  @Override
  public void clear() {
    setClocks(null);
  }
}
