package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.server.data.handler.GetRowsHandler;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.GetUDFResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetPSFRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetUDFHandler extends Handler {

  private static final Log LOG = LogFactory.getLog(GetRowsHandler.class);

  public GetUDFHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    GetUDFResponse response = new GetUDFResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    GetPSFRequest getPSFRequest = (GetPSFRequest) userRequest;
    MapResponseCache cache = (MapResponseCache) responseCache;

    // Adaptor to Get PSF merge
    ConcurrentHashMap<Request, Response> responses = cache
        .getResponses();
    int responseNum = responses.size();
    List<PartitionGetResult> partGetResults = new ArrayList<>(responseNum);
    for (Response response : responses.values()) {
      partGetResults.add(((GetUDFResponse) response.getData()).getPartResult());
    }

    // Merge the sub-results
    try {
      finalResult.set(getPSFRequest.getGetFunc().merge(partGetResults));
    } catch (Exception x) {
      LOG.error("merge row failed ", x);
      finalResult.setExecuteException(new ExecutionException(x));
    }
  }
}
