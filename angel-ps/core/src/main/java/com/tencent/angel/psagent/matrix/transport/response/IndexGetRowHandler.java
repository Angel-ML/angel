package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowRequest;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.StreamIndexPartGetRowResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.IndexGetRowRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.MergeUtils;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import io.netty.buffer.ByteBuf;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IndexGetRowHandler extends Handler {

  private static final Log LOG = LogFactory.getLog(IndexGetRowHandler.class);

  public IndexGetRowHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    StreamIndexPartGetRowResponse response = new StreamIndexPartGetRowResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    IndexGetRowRequest indexGetRowRequest = (IndexGetRowRequest) userRequest;
    MapResponseCache cache = (MapResponseCache) responseCache;
    ConcurrentHashMap<Request, Response> responses = cache.getResponses();

    // Merge
    Vector result = merge(indexGetRowRequest, responses);

    // Set matrix/row meta
    result.setMatrixId(indexGetRowRequest.getMatrixId());
    result.setRowId(indexGetRowRequest.getRowId());

    // Set final result
    finalResult.set(result);
  }

  private Vector merge(IndexGetRowRequest indexGetRowRequest,
      ConcurrentHashMap<Request, Response> responses) {
    KeyPart[] keyParts = new KeyPart[responses.size()];
    ValuePart[] valueParts = new ValuePart[responses.size()];
    int index = 0;
    for (Entry<Request, Response> entry : responses.entrySet()) {
      keyParts[index] = ((IndexPartGetRowRequest) (entry.getKey().getData())).getKeyPart();
      valueParts[index] = ((StreamIndexPartGetRowResponse) (entry.getValue().getData())).getValuePart();
      index++;
    }

    return MergeUtils.combineIndexRowSplits(indexGetRowRequest.getMatrixId(),
        indexGetRowRequest.getRowId(), indexGetRowRequest.size(), keyParts, valueParts);
  }

}
