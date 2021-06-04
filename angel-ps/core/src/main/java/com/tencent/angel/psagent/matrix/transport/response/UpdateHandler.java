package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.matrix.psf.update.base.VoidResult;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.server.data.response.ResponseType;
import com.tencent.angel.ps.server.data.response.UpdateResponse;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import io.netty.buffer.ByteBuf;

public class UpdateHandler extends Handler {

  public UpdateHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    // Just
    UpdateResponse response = new UpdateResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    MapResponseCache cache = (MapResponseCache) responseCache;

    // Check update result
    boolean success = true;
    String detail = "";
    for (Response response : cache.getResponses().values()) {
      success = success && (response.getResponseType() == ResponseType.SUCCESS);
      if (!success) {
        detail = response.getDetail();
        break;
      }
    }

    // Set the final result
    if (success) {
      finalResult.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.SUCCESS));
    } else {
      finalResult.set(new VoidResult(com.tencent.angel.psagent.matrix.ResponseType.FAILED, detail));
    }
  }
}
