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

package com.tencent.angel.master.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.ipc.TConnection;
import com.tencent.angel.ipc.TConnectionManager;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.protobuf.generated.MasterPSServiceProtos.GetThreadStackRequest;
import com.tencent.angel.ps.PSAttemptId;
import com.tencent.angel.ps.impl.PSProtocol;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.IOException;

/**
 * Master to ps rpc client, just for get thread stack of ps.
 */
public class PSClient implements PSClientInterface {
  private static final Log LOG = LogFactory.getLog(PSClient.class);
  /**master context*/
  private final AMContext context;

  /**connection from master to the worker*/
  private TConnection connection;

  /**rpc protocol*/
  private PSProtocol ps;

  /**
   * Create a PSClient
   * @param context master context
   * @param attemptId ps attempt id
   * @throws IOException
   */
  public PSClient(AMContext context, PSAttemptId attemptId) throws IOException {
    this.context = context;
    this.connection = TConnectionManager.getConnection(context.getConf());

    Location psLoc =
        context.getParameterServerManager().getParameterServer(attemptId.getPsId())
            .getPSAttempt(attemptId).getLocation();
    LOG.info("psLoc= " + psLoc.toString());
    LOG.info("psLoc.getIp()=    " + psLoc.getIp() + "   " + "          psLoc.getPort()=   "
        + psLoc.getPort());
    this.ps = connection.getPSService(psLoc.getIp(), psLoc.getPort());
  }

  @Override
  public String getThreadStack() throws ServiceException {
    PSProtocol pSProtocol = getPS();
    GetThreadStackRequest request = GetThreadStackRequest.newBuilder().build();
    return pSProtocol.psThreadStack(null, request).getStack();
  }

  private PSProtocol getPS() {
    return ps;
  }

}
