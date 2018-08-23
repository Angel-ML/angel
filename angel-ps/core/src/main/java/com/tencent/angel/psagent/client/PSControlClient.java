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


package com.tencent.angel.psagent.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos;
import com.tencent.angel.ps.server.control.PSProtocol;
import com.tencent.angel.ps.server.data.ServerState;
import com.tencent.angel.psagent.PSAgentContext;

import java.io.IOException;

public class PSControlClient {
  private volatile PSProtocol ps;

  public PSControlClient() {

  }

  public void init(Location loc) throws IOException {
    ps = PSAgentContext.get().getControlConnectManager().getPSService(loc.getIp(), loc.getPort());
  }

  public ServerState getState() throws ServiceException {
    return ServerState.valueOf(ps.getState(null, PSAgentPSServiceProtos.GetStateRequest.newBuilder()
      .setClientId(PSAgentContext.get().getPSAgentId()).build()).getState());
  }

  public int getToken(int dateSize) throws ServiceException {
    return ps.getToken(null, PSAgentPSServiceProtos.GetTokenRequest.newBuilder()
      .setClientId(PSAgentContext.get().getPSAgentId()).setDataSize(dateSize).build()).getToken();
  }
}
