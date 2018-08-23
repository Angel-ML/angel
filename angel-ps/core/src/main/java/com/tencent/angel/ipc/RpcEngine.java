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

import org.apache.hadoop.conf.Configuration;

import javax.net.SocketFactory;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.List;


/**
 * An RPC implementation.
 */
public interface RpcEngine {

  /**
   * Construct a client-side proxy object.
   */
  VersionedProtocol getProxy(Class<? extends VersionedProtocol> protocol, long clientVersion,
    InetSocketAddress addr, Configuration conf, SocketFactory factory, int rpcTimeout,
    List<String> addrList4Failover) throws IOException;

  /**
   * Stop this proxy.
   */
  void stopProxy(VersionedProtocol proxy);

  /**
   * Construct a server for a protocol implementation instance.
   */
  RpcServer getServer(Class<? extends VersionedProtocol> protocol, Object instance,
    Class<?>[] ifaces, String bindAddress, int port, Configuration conf) throws IOException;

  /**
   * Stop all workers and clear resources
   */
  void shutDown();
}
