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

import com.tencent.angel.common.location.Location;
import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * PS RPC request running context.
 */
public class RequestContext {
  /** request last fime time */
  private long failedTs;

  /** The server the request send to */
  private volatile ParameterServerId serverId;

  /**
   * The actual PS that the request send to
   */
  private volatile ParameterServerId actualServerId;

  /** The actual PS location */
  private volatile Location location;

  /** netty channel allocated to this request */
  private volatile Channel channel;

  /** netty channel pool for this request */
  private volatile GenericObjectPool<Channel> channelPool;

  /** time ticks of waiting for request result, it used to check the request is timeout or not */
  private int waitTimeTicks;

  private volatile long submitStartTs = -1L;

  private volatile long getChannelStartTs = -1L;

  private volatile long serializeStartTs = -1L;

  private volatile long sendStartTs = -1L;

  /** Buf that save the serialized request */
  private volatile ByteBuf serializedData;

  private volatile long nextRetryTs = -1L;

  /**
   * Create a new RequestContext.
   */
  public RequestContext() {
    failedTs = -1;
    channel = null;
    channelPool = null;
    waitTimeTicks = 0;
  }

  /**
   * Get the last failed time.
   * 
   * @return int the last failed time
   */
  public long getFailedTs() {
    return failedTs;
  }

  /**
   * Get the netty channel allocated to this request.
   * 
   * @return the netty channel allocated to this request
   */
  public Channel getChannel() {
    return channel;
  }

  /**
   * Get the netty channel pool for this request.
   * 
   * @return GenericObjectPool<Channel> the netty channel pool for this request
   */
  public GenericObjectPool<Channel> getChannelPool() {
    return channelPool;
  }

  /**
   * Get the time ticks of waiting for request result.
   * 
   * @return int the time ticks of waiting for request result
   */
  public int getWaitTimeTicks() {
    return waitTimeTicks;
  }

  /**
   * Set the failed time of the request.
   * 
   * @param failedTs the failed time of the request
   */
  public void setFailedTs(long failedTs) {
    this.failedTs = failedTs;
  }

  /**
   * Set the netty channel allocated to the request.
   * 
   * @param channel the netty channel allocated to the request
   */
  public void setChannel(Channel channel) {
    this.channel = channel;
  }

  /**
   * Set the netty channel pool for this request.
   * 
   * @param channelPool the netty channel pool for this request
   */
  public void setChannelPool(GenericObjectPool<Channel> channelPool) {
    this.channelPool = channelPool;
  }

  /**
   * Set the time ticks of waiting for request result.
   * 
   * @param waitTimeTicks the time ticks of waiting for request result
   */
  public void setWaitTimeTicks(int waitTimeTicks) {
    this.waitTimeTicks = waitTimeTicks;
  }

  /**
   * Increment wait time ticks.
   * 
   * @param ticks increment value
   */
  public void addWaitTimeTicks(int ticks) {
    waitTimeTicks += ticks;
  }

  /**
   * Get the ps that send the request
   * @return the ps that send the request
   */
  public ParameterServerId getServerId() {
    return serverId;
  }

  /**
   * Set the ps that send the request
   * @param serverId the ps that send the request
   */
  public void setServerId(ParameterServerId serverId) {
    this.serverId = serverId;
  }

  /**
   * Get the location of ps that send the request
   * @return the location of ps that send the request
   */
  public Location getLocation() {
    return location;
  }

  /**
   * Set the location of ps that send the request
   * @param location the location of ps that send the request
   */
  public void setLocation(Location location) {
    this.location = location;
  }

  /**
   * Get the actual PS that the request send to
   * @return the actual PS that the request send to
   */
  public ParameterServerId getActualServerId() {
    return actualServerId;
  }

  /**
   * Set the actual PS that the request send to
   * @param actualServerId the actual PS that the request send to
   */
  public void setActualServerId(ParameterServerId actualServerId) {
    this.actualServerId = actualServerId;
  }

  /**
   * Get the serialized request data buffer
   * @return the serialized request data buffer
   */
  public ByteBuf getSerializedData() {
    return serializedData;
  }

  /**
   * Set the serialized request data buffer
   * @param serializedData the serialized request data buffer
   */
  public void setSerializedData(ByteBuf serializedData) {
    this.serializedData = serializedData;
  }

  /**
   * Set send start timestamp
   * @param ts start timestamp
   */
  public void setSendStartTs(long ts) {
    this.sendStartTs = ts;
  }

  /**
   * Get send start timestamp
   * @return send start timestamp
   */
  public long getSendStartTs() {
    return sendStartTs;
  }

  /**
   * Reset context
   */
  public void reset() {
    sendStartTs = -1;
    failedTs = -1;
    waitTimeTicks = 0;
  }

  /**
   * Set next retry ts
   * @param nextRetryTs next retry ts
   */
  public void setNextRetryTs(long nextRetryTs) {
    this.nextRetryTs = nextRetryTs;
  }

  /**
   * Get next retry ts
   * @return next retry ts
   */
  public long getNextRetryTs() {
    return nextRetryTs;
  }
}
