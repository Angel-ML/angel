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

import io.netty.channel.Channel;
import org.apache.commons.pool.impl.GenericObjectPool;

/**
 * PS RPC request running context.
 */
public class RequestContext {
  /** request last fime time */
  private long failedTs;

  /** netty channel allocated to this request */
  private volatile Channel channel;

  /** netty channel pool for this request */
  private volatile GenericObjectPool<Channel> channelPool;

  /** time ticks of waiting for request result, it used to check the request is timeout or not */
  private int waitTimeTicks;

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
   * @param GenericObjectPool<Channel> the netty channel pool for this request
   */
  public void setChannelPool(GenericObjectPool<Channel> channelPool) {
    this.channelPool = channelPool;
  }

  /**
   * Set the time ticks of waiting for request result.
   * 
   * @param int the time ticks of waiting for request result
   */
  public void setWaitTimeTicks(int waitTimeTicks) {
    this.waitTimeTicks = waitTimeTicks;
  }

  /**
   * Increment wait time ticks.
   * 
   * @param int increment value
   */
  public void addWaitTimeTicks(int ticks) {
    waitTimeTicks += ticks;
  }
}
