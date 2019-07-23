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


package com.tencent.angel.ps.server.control;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.tencent.angel.ipc.MLRPC;
import com.tencent.angel.ipc.RpcServer;
import com.tencent.angel.protobuf.generated.MasterPSServiceProtos.GetThreadStackRequest;
import com.tencent.angel.protobuf.generated.MasterPSServiceProtos.GetThreadStackResponse;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos.GetStateRequest;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos.GetStateResponse;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos.GetTokenRequest;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos.GetTokenResponse;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.utils.ThreadUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;
import java.net.UnknownHostException;

/**
 * The Parameter server service.
 */
public class ParameterServerService implements PSProtocol {

  private static final Log LOG = LogFactory.getLog(ParameterServerService.class);
  //private final ParameterServer psServer;
  private RpcServer rpcServer;

  private PSContext context;

  public ParameterServerService(PSContext context) {
    this.context = context;
  }

  /**
   * Gets host address.
   *
   * @return the host address
   * @throws UnknownHostException
   */
  public String getHostAddress() throws UnknownHostException {
    return InetAddress.getLocalHost().getHostAddress();
  }

  /**
   * Gets port.
   *
   * @return the port
   */
  public int getPort() {
    return rpcServer.getPort();
  }

  /**
   * Start rpc server
   *
   * @throws IOException the io exception
   */
  public void start() throws IOException {
    Configuration conf = context.getConf();
    int psServerPort = NetUtils.chooseAListenPort(conf);
    String psServerHost = InetAddress.getLocalHost().getHostAddress();
    rpcServer = MLRPC
      .getServer(ParameterServerService.class, this, new Class<?>[] {PSProtocol.class},
        psServerHost, psServerPort, conf);
    LOG.info("Starting parameter server service at " + psServerHost + ":" + psServerPort);
    rpcServer.openServer();
  }

  public void stop() {
    if (rpcServer != null) {
      rpcServer.stop();
    }
  }

  @Override public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0;
  }

  @Override public GetThreadStackResponse psThreadStack(RpcController controller,
    GetThreadStackRequest request) throws ServiceException {
    String stackTraceInfoString = getThreadStack();
    GetThreadStackResponse getThreadStackResponse =
      GetThreadStackResponse.newBuilder().setStack(stackTraceInfoString).build();
    return getThreadStackResponse;
  }

  /**
   * Gets thread stack.
   *
   * @return the thread stack
   */
  private String getThreadStack() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfo = threadMXBean.dumpAllThreads(true, true);
    StringBuilder stackTraceString = new StringBuilder("ParameterServer\n");
    for (ThreadInfo t : threadInfo) {
      stackTraceString.append(ThreadUtils.toString(t)).append("\n\n");
    }
    return stackTraceString.toString();
  }

  @Override public GetStateResponse getState(RpcController controller, GetStateRequest request)
    throws ServiceException {
    return GetStateResponse.newBuilder()
      .setState(context.getRunningContext().getState().getTypeId()).build();
  }

  @Override public GetTokenResponse getToken(RpcController controller, GetTokenRequest request)
    throws ServiceException {
    return GetTokenResponse.newBuilder().setToken(
      context.getRunningContext().allocateToken(request.getClientId(), request.getDataSize()))
      .build();
  }
}