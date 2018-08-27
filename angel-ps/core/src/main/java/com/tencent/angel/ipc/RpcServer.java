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


package com.tencent.angel.ipc;

import com.google.protobuf.Message;
import com.tencent.angel.protobuf.generated.RPCProtos;

import java.io.IOException;
import java.net.InetSocketAddress;

public interface RpcServer {
  /**
   * The port this server runs on.
   */
  int getPort();

  void start();

  void stop();

  void join() throws InterruptedException;

  /**
   * Called for each call.
   *
   * @param param       writable parameter
   * @param receiveTime time
   * @return Message
   * @throws java.io.IOException e
   */
  Message call(Class<? extends VersionedProtocol> protocol, RPCProtos.RpcRequestBody param,
    long receiveTime) throws IOException;

  InetSocketAddress getListenerAddress();

  public void openServer();

  /**
   * Total number of clients connect to this server.
   */
  public int getNumberOfConnections();


  void addProtocolImpl(Class<?> protocol, Object impl);

}
