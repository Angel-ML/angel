package com.tencent.angel.psagent.client;

import com.google.protobuf.ServiceException;
import com.tencent.angel.common.location.Location;
import com.tencent.angel.ml.matrix.transport.PSLocation;
import com.tencent.angel.ml.matrix.transport.ServerState;
import com.tencent.angel.protobuf.generated.PSAgentPSServiceProtos;
import com.tencent.angel.ps.impl.PSProtocol;
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
    return ServerState.valueOf(ps.getState(null,
      PSAgentPSServiceProtos.GetStateRequest.newBuilder().setClientId(PSAgentContext.get().getPSAgentId()).build()).getState());
  }

  public int getToken(int dateSize) throws ServiceException {
    return ps.getToken(null,
      PSAgentPSServiceProtos.GetTokenRequest.newBuilder().setClientId(PSAgentContext.get().getPSAgentId()).setDataSize(dateSize).build()).getToken();
  }
}
