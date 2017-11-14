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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.master.workerclient;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.Location;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.protobuf.generated.MasterWorkerServiceProtos.GetThreadStackRequest;
import com.tencent.angel.worker.WorkerAttemptId;
import com.tencent.angel.worker.WorkerProtocol;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * The RPC client from master to a worker, it just for get worker thread stack from worker.
 */
public class WorkerClient implements WorkerClientInterface{
  private static final Log LOG = LogFactory.getLog(WorkerClient.class);
  /**master context*/
  private final AMContext context;

  /**connection from master to the worker*/
  private final TConnection connection;

  /**rpc protocol*/
  private final WorkerProtocol worker;

  /**
   * Create a WorkerClient
   * @param context master context
   * @param workerAttemptId worker attempt id
   * @throws IOException
   */
  public WorkerClient(AMContext context, WorkerAttemptId workerAttemptId) throws IOException {
    this.context = context;
    this.connection = TConnectionManager.getConnection(context.getConf());
    Location workerLoc =
        context.getWorkerManager().getWorker(workerAttemptId.getWorkerId())
            .getWorkerAttempt(workerAttemptId).getLocation();
    LOG.debug("workerLoc= " + workerLoc.toString());
    this.worker = connection.getWorkerService(workerLoc.getIp(), workerLoc.getPort());
  }

  @Override
  public String getThreadStack() throws ServiceException {
    WorkerProtocol workerProtocol = getWorker();
    GetThreadStackRequest request = GetThreadStackRequest.newBuilder().build();
    LOG.info("the class of workerProtocol is " + workerProtocol.getClass());
    return workerProtocol.workerThreadStack(null, request).getStack();
  }

  private WorkerProtocol getWorker() {
    return worker;
  }
}
