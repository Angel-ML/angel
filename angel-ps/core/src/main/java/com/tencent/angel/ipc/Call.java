/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tencent.angel.ipc;

import org.jboss.netty.channel.ChannelHandlerContext;
import org.jboss.netty.channel.MessageEvent;

public class Call {
  private ChannelHandlerContext ctx;

  private MessageEvent e;

  /**
   * @param ctx
   * @param e
   */
  public Call(ChannelHandlerContext ctx, MessageEvent e) {
    super();
    this.ctx = ctx;
    this.e = e;
  }

  /**
   * @return the ctx
   */
  public ChannelHandlerContext getCtx() {
    return ctx;
  }

  /**
   * @param ctx the ctx to set
   */
  public void setCtx(ChannelHandlerContext ctx) {
    this.ctx = ctx;
  }

  /**
   * @return the e
   */
  public MessageEvent getE() {
    return e;
  }

  /**
   * @param e the e to set
   */
  public void setE(MessageEvent e) {
    this.e = e;
  }
}
