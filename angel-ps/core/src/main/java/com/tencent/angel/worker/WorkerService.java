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


package com.tencent.angel.worker;

import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.ipc.MLRPC;
import com.tencent.angel.ipc.RpcServer;
import com.tencent.angel.protobuf.generated.MasterWorkerServiceProtos.GetThreadStackRequest;
import com.tencent.angel.protobuf.generated.MasterWorkerServiceProtos.GetThreadStackResponse;
import com.tencent.angel.protobuf.generated.WorkerWorkerServiceProtos.*;
import com.tencent.angel.utils.NetUtils;
import com.tencent.angel.utils.ThreadUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;


/**
 * Worker service
 */
public class WorkerService implements WorkerProtocol {

  private static final Log LOG = LogFactory.getLog(WorkerService.class);

  /**
   * How many different ports we try when the rpc bind loses a port race
   */
  private static final int MAX_BIND_TRY = 10;

  private RpcServer rpcServer;
  private volatile Location location;

  public WorkerService() {

  }

  @Override public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0;
  }

  public void start() throws IOException {
    // Prefer an explicitly configured host (set via angel.worker.listen.host in the
    // job conf, typically injected by StandaloneContainerLauncher). Falls back to
    // NetUtils.getRealLocalIP() which itself scans interfaces when
    // InetAddress.getLocalHost() returns a loopback due to /etc/hosts misconfig.
    String workerServerHost = WorkerContext.get().getConf().get(
        com.tencent.angel.conf.AngelConf.ANGEL_WORKER_LISTEN_HOST);
    if (workerServerHost == null || workerServerHost.isEmpty()) {
      workerServerHost = NetUtils.getRealLocalIP();
    }

    // Reserve the port with a real listening socket and release it only right before the rpc
    // bind, otherwise another Angel process drawing from the same random port range can grab
    // it in between and the bind dies with "Address already in use".
    Throwable lastError = null;
    for (int i = 1; i <= MAX_BIND_TRY; i++) {
      NetUtils.PortReservation reservation =
          NetUtils.reserveListenPortPair(WorkerContext.get().getConf());
      int workerServerPort = reservation.getPort();
      reservation.close();
      try {
        rpcServer = MLRPC
          .getServer(WorkerService.class, this, new Class<?>[] {WorkerProtocol.class},
            workerServerHost, workerServerPort, WorkerContext.get().getConf());
        location = new Location(workerServerHost, workerServerPort);
        LOG.info("Starting workerserver service at " + workerServerHost + ":" + workerServerPort);
        rpcServer.openServer();
        return;
      } catch (Throwable x) {
        if (rpcServer != null) {
          try {
            rpcServer.stop();
          } catch (Throwable ignore) {
            // best effort
          }
          rpcServer = null;
        }
        lastError = x;
        if (!NetUtils.isAddressInUse(x)) {
          break;
        }
        LOG.warn("Bind workerserver service on " + workerServerHost + ":" + workerServerPort
          + " failed (attempt " + i + "/" + MAX_BIND_TRY + "), choose another port", x);
      }
    }

    if (lastError instanceof IOException) {
      throw (IOException) lastError;
    }
    throw new IOException(
        "Start workerserver service failed after " + MAX_BIND_TRY + " attempts", lastError);
  }

  public void stop() {
    LOG.info("stop rpc server");
    if (rpcServer != null) {
      rpcServer.stop();
    }
  }


  public GetThreadStackResponse workerThreadStack(RpcController controller,
    GetThreadStackRequest request) throws ServiceException {
    String stackTraceInfoString = getThreadStack();
    GetThreadStackResponse getThreadStackResponse =
      GetThreadStackResponse.newBuilder().setStack(stackTraceInfoString).build();
    return getThreadStackResponse;
  }

  private String getThreadStack() {
    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfo = threadMXBean.dumpAllThreads(true, true);
    StringBuilder stackTraceString = new StringBuilder("Worker\n");
    for (ThreadInfo t : threadInfo) {
      stackTraceString.append(ThreadUtils.toString(t));
      stackTraceString.append("\n\n");
    }
    return stackTraceString.toString();
  }


  @Override public ActionResponse action(RpcController controller, ActionRequest request)
    throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public ActionResultResponse actionResult(RpcController controller, ActionResultRequest request)
    throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }


  @Override public UpdateResponse update(RpcController controller, UpdateRequest request)
    throws ServiceException {
    // TODO Auto-generated method stub
    return null;
  }

  /**
   * Get worker location(ip and listening port)
   *
   * @return Location worker location(ip and listening port)
   */
  public Location getLocation() {
    return location;
  }
}
