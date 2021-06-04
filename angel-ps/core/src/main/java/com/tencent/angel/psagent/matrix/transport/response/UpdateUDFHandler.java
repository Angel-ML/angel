package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;

import com.tencent.angel.ps.server.data.response.ResponseType;
import com.tencent.angel.ps.server.data.response.UpdateUDFResponse;
import com.tencent.angel.psagent.PSAgentContext;

import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import io.netty.buffer.ByteBuf;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class UpdateUDFHandler extends Handler {

  public UpdateUDFHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    UpdateUDFResponse response = new UpdateUDFResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    MapResponseCache cache = (MapResponseCache) responseCache;

    // Check update result
    Map<Request, Response> responses = cache.getResponses();
    boolean success = true;
    String detail = "";
    for(Response response : responses.values()) {
      success = success && (response.getResponseType() == ResponseType.SUCCESS);
      if(!success) {
        detail = response.getDetail();
        break;
      }
    }

    // Set the final result
    if(success) {
      finalResult.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
    } else {
      finalResult.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.FAILED, detail));
    }
  }
}
