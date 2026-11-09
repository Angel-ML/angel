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
import java.net.UnknownHostException;

/**
 * The Parameter server service.
 */
public class ParameterServerService implements PSProtocol {

  private static final Log LOG = LogFactory.getLog(ParameterServerService.class);

  /**
   * How many different port pairs we try when the rpc bind loses a port race
   */
  private static final int MAX_BIND_TRY = 10;

  //private final ParameterServer psServer;
  private RpcServer rpcServer;

  /**
   * Reservation of the port pair: the control half is released before the rpc bind, the data
   * half ({@code port + 1}) stays held until the matrix-transport server binds it.
   */
  private volatile NetUtils.PortReservation portReservation;

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
    return resolveListenHost(context.getConf());
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
    String psServerHost = resolveListenHost(conf);

    // The PS uses TWO ports: `port` for the control rpc server (bound right here) and
    // `port + 1` for the matrix-transport data server, which is only bound later in
    // ParameterServer#start(). Both are reserved up-front by real listening sockets so no
    // other process on this host can steal them between selection and bind; the data-port
    // half stays held until MatrixTransportServer binds it.
    Throwable lastError = null;
    for (int i = 1; i <= MAX_BIND_TRY; i++) {
      NetUtils.PortReservation reservation = NetUtils.reserveListenPortPair(conf);
      int psServerPort = reservation.getPort();
      // Hand the control port over to the rpc server.
      reservation.releaseControlPort();
      try {
        rpcServer = MLRPC
          .getServer(ParameterServerService.class, this, new Class<?>[] {PSProtocol.class},
            psServerHost, psServerPort, conf);
        LOG.info("Starting parameter server service at " + psServerHost + ":" + psServerPort
          + ", data port " + reservation.getDataPort() + " reserved");
        rpcServer.openServer();
        this.portReservation = reservation;
        return;
      } catch (Throwable x) {
        // The port was free a moment ago but somebody won the race: drop the reservation and
        // draw another port pair instead of failing the whole PS startup.
        reservation.close();
        stopRpcServerQuietly();
        lastError = x;
        if (!NetUtils.isAddressInUse(x)) {
          break;
        }
        LOG.warn("Bind parameter server service on " + psServerHost + ":" + psServerPort
          + " failed (attempt " + i + "/" + MAX_BIND_TRY + "), choose another port", x);
      }
    }

    if (lastError instanceof IOException) {
      throw (IOException) lastError;
    }
    throw new IOException("Start parameter server service failed after " + MAX_BIND_TRY
      + " attempts", lastError);
  }

  private void stopRpcServerQuietly() {
    if (rpcServer != null) {
      try {
        rpcServer.stop();
      } catch (Throwable ignore) {
        // best effort
      }
      rpcServer = null;
    }
  }

  /**
   * The still-held reservation of the data port ({@code control port + 1}). The
   * matrix-transport server must release it right before it binds.
   *
   * @return the reservation, or null if the rpc server has not been started
   */
  public NetUtils.PortReservation getPortReservation() {
    return portReservation;
  }


  /**
   * Resolve the host the PS should bind its RPC server to and advertise to the master.
   * Explicit configuration wins over {@link InetAddress#getLocalHost()} / network-interface
   * scanning; see {@link com.tencent.angel.conf.AngelConf#ANGEL_PS_LISTEN_HOST}.
   */
  private static String resolveListenHost(Configuration conf) throws UnknownHostException {
    String host = conf.get(com.tencent.angel.conf.AngelConf.ANGEL_PS_LISTEN_HOST);
    if (host != null && !host.isEmpty()) {
      return host;
    }
    return NetUtils.getRealLocalIP();
  }

  public void stop() {
    if (portReservation != null) {
      portReservation.close();
      portReservation = null;
    }
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