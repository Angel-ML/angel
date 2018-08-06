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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.net.InetAddress;


/**
 * Worker service
 */
public class WorkerService implements WorkerProtocol {

  private static final Log LOG = LogFactory.getLog(WorkerService.class);
  private RpcServer rpcServer;
  private volatile Location location;

  public WorkerService() {

  }

  @Override
  public long getProtocolVersion(String protocol, long clientVersion) throws IOException {
    return 0;
  }

  public void start() throws IOException {
    int workerServerPort = NetUtils.chooseAListenPort(WorkerContext.get().getConf());
    String workerServerHost = InetAddress.getLocalHost().getHostAddress();
    location = new Location(workerServerHost, workerServerPort);
    rpcServer =
        MLRPC.getServer(WorkerService.class, this, new Class<?>[] {WorkerProtocol.class},
            workerServerHost, workerServerPort, WorkerContext.get().getConf());
    LOG.info("Starting workerserver service at " + workerServerHost + ":" + workerServerPort);
    rpcServer.openServer();
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
    StringBuilder infoBlock = new StringBuilder("\n");
    for (ThreadInfo t : threadInfo) {
      infoBlock = new StringBuilder("\n\n");
      infoBlock.append("threadid: ").append(t.getThreadId()).append("   threadname: ").append(t.getThreadName()).append("       threadstate: ").append(t.getThreadState()).append("\n");
      for (StackTraceElement stackTraceElement : t.getStackTrace()) {
        infoBlock.append("   ").append(stackTraceElement.toString()).append("\n");
      }
      stackTraceString.append(infoBlock).append("\n\n");
    }
    return stackTraceString.toString();
  }


  @Override
  public ActionResponse action(RpcController controller, ActionRequest request)
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


  @Override
  public UpdateResponse update(RpcController controller, UpdateRequest request)
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
