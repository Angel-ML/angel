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


package com.tencent.angel.common.transport;

import com.tencent.angel.common.location.Location;
import io.netty.channel.Channel;

/**
 * Netty channel wrapper
 */
public class NettyChannel {
  /**
   * Netty channel last use timestamp
   */
  private volatile long lastUseTs;

  /**
   * Netty channel
   */
  private final Channel channel;

  /**
   * Server location
   */
  private final Location loc;

  /**
   * Is in use or not
   */
  private volatile boolean inUse;

  /**
   * Create a new netty channel wrapper
   *
   * @param channel netty channel
   * @param loc     server location
   */
  public NettyChannel(Channel channel, Location loc) {
    this.channel = channel;
    this.loc = loc;
    this.inUse = false;
    this.lastUseTs = System.currentTimeMillis();
  }

  /**
   * Get server location
   *
   * @return server location
   */
  public Location getLoc() {
    return loc;
  }

  /**
   * Is channel usable
   *
   * @return true means usable
   */
  public boolean isUseable() {
    return !inUse;
  }

  /**
   * Set the channel in use
   */
  public void setInUse() {
    inUse = true;
    lastUseTs = System.currentTimeMillis();
  }

  /**
   * Set the channel in unused
   */
  public void setUnUse() {
    inUse = false;
    lastUseTs = System.currentTimeMillis();
  }

  /**
   * Close the channel
   */
  public void close() {
    try {
      channel.close();
    } catch (Throwable x) {

    }
  }

  /**
   * Get netty channel
   *
   * @return netty channel
   */
  public Channel getChannel() {
    return channel;
  }

  /**
   * Get the last use timestamp of the channel
   *
   * @return the last use timestamp of the channel
   */
  public long getLastUseTs() {
    return lastUseTs;
  }
}
