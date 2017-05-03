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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.matrix.transport;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;

public abstract class PartitionRequest extends Request {
  protected int clock;
  protected PartitionKey partKey;

  public PartitionRequest(ParameterServerId serverId, int clock, PartitionKey partKey) {
    super(serverId);
    this.clock = clock;
    this.partKey = partKey;
  }

  public PartitionRequest() {
    this(null, 0, null);
  }

  public int getClock() {
    return clock;
  }

  public PartitionKey getPartKey() {
    return partKey;
  }

  public void setClock(int clock) {
    this.clock = clock;
  }

  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }

  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(clock);
    partKey.serialize(buf);
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    clock = buf.readInt();
    partKey = new PartitionKey();
    partKey.deserialize(buf);
  }

  @Override
  public int bufferLen() {
    if (partKey != null) {
      return super.bufferLen() + 4 + partKey.bufferLen();
    } else {
      return super.bufferLen() + 4;
    }
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + clock;
    result = prime * result + ((partKey == null) ? 0 : partKey.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    PartitionRequest other = (PartitionRequest) obj;
    if (clock != other.clock)
      return false;
    if (partKey == null) {
      if (other.partKey != null)
        return false;
    } else if (!partKey.equals(other.partKey))
      return false;
    return true;
  }

  @Override
  public String toString() {
    return "PartitionRequest [clock=" + clock + ", partKey=" + partKey + "]";
  }
}
