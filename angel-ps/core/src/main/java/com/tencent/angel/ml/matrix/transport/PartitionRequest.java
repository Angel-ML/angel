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

/**
 * The base class of rpc request. The operation object of general rpcs between PSAgent and PS in
 * Angel is a matrix partition.
 */
public abstract class PartitionRequest extends Request {
  /** clock value, for consistency control if needed */
  protected int clock;

  /** matrix partition key */
  protected PartitionKey partKey;

  /** Is the request come from ps */
  protected boolean comeFromPs;

  /** The ps that send the request */
  //protected ParameterServerId psId;

  /** The location of ps that send the request */
  //protected Location location;

  /**
   * Create a new PartitionRequest.
   *
   * @param clock clock value
   * @param partKey matrix partition key
   */
  public PartitionRequest(int clock, PartitionKey partKey) {
    super(new RequestContext());
    this.clock = clock;
    this.partKey = partKey;
  }

  /**
   * Create a new PartitionRequest.
   */
  public PartitionRequest() {
    this(0, null);
  }

  /**
   * Get clock value.
   * 
   * @return int clock value
   */
  public int getClock() {
    return clock;
  }

  /**
   * Get matrix partition key.
   * 
   * @return PartitionKey matrix partition key
   */
  public PartitionKey getPartKey() {
    return partKey;
  }

  /**
   * Set clock value.
   * 
   * @param clock clock value
   */
  public void setClock(int clock) {
    this.clock = clock;
  }

  /**
   * Set matrix partition key.
   * 
   * @param partKey matrix partition key
   */
  public void setPartKey(PartitionKey partKey) {
    this.partKey = partKey;
  }

  /**
   * Is come from ps
   * @return true means this request come from a ps
   */
  public boolean isComeFromPs() {
    return comeFromPs;
  }

  /**
   * Set is the request come from a ps
   * @param comeFromPs is the request come from a ps
   */
  public void setComeFromPs(boolean comeFromPs) {
    this.comeFromPs = comeFromPs;
  }


  @Override
  public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeBoolean(comeFromPs);
    buf.writeInt(clock);
    partKey.serialize(buf);
    //if(comeFromPs) {
    //  buf.writeInt(psId.getIndex());
    //  byte[] data = location.getIp().getBytes();
    //  buf.writeInt(data.length);
    //  buf.writeBytes(data);
    //  buf.writeInt(location.getPort());
    //}
  }

  @Override
  public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    comeFromPs = buf.readBoolean();
    clock = buf.readInt();
    partKey = new PartitionKey();
    partKey.deserialize(buf);

    //if(comeFromPs) {
    //  psId = new ParameterServerId(buf.readInt());
    //  int size = buf.readInt();
    //  byte[] data = new byte[size];
    //  buf.readBytes(data);
    //  location = new Location(new String(data), buf.readInt());
    //}
  }

  @Override
  public int bufferLen() {
    if (partKey != null) {
      return super.bufferLen() + 4 + partKey.bufferLen();
    } else {
      return super.bufferLen() + 4;
    }
  }

  @Override public String toString() {
    return "PartitionRequest{" + "clock=" + clock + ", partKey=" + partKey + ", comeFromPs="
      + comeFromPs + "} " + super.toString();
  }

  @Override public boolean equals(Object o) {
    if (this == o)
      return true;
    if (o == null || getClass() != o.getClass())
      return false;

    PartitionRequest that = (PartitionRequest) o;

    if (clock != that.clock)
      return false;
    if (comeFromPs != that.comeFromPs)
      return false;
    return partKey != null ? partKey.equals(that.partKey) : that.partKey == null;
  }

  @Override public int hashCode() {
    int result = clock;
    result = 31 * result + (partKey != null ? partKey.hashCode() : 0);
    result = 31 * result + (comeFromPs ? 1 : 0);
    return result;
  }
}
