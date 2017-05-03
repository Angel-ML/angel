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

package com.tencent.angel.psagent.matrix.transport;

import com.tencent.angel.common.Location;
import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.pool.PoolableObjectFactory;

/**
 * Netty channel pool factory.
 */
public class ChannelObjectFactory implements PoolableObjectFactory<Channel> {
  private static final Log LOG = LogFactory.getLog(ChannelObjectFactory.class);
  /**server address*/
  private final Location loc;
  
  /**netty client bootstrap*/
  private final Bootstrap bootstrap;

  /**
   * Create a new ChannelObjectFactory.
   *
   * @param loc server address
   * @param bootstrap netty client bootstrap
   */
  public ChannelObjectFactory(Location loc, Bootstrap bootstrap) {
    this.loc = loc;
    this.bootstrap = bootstrap;
  }

  @Override
  public void activateObject(Channel channel) throws Exception {

  }

  @Override
  public void destroyObject(Channel channel) throws Exception {
    channel.close();
  }

  @Override
  public Channel makeObject() throws Exception {
    Channel ch = bootstrap.connect(loc.getIp(), loc.getPort() + 1).sync().channel();
    LOG.debug("connect success for " + loc.getIp() + ":" + (loc.getPort() + 1));
    return ch;
  }

  @Override
  public void passivateObject(Channel channel) throws Exception {

  }

  @Override
  public boolean validateObject(Channel channel) {
    return channel.isOpen();
  }
}
