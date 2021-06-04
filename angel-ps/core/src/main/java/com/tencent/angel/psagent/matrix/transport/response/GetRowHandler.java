package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.server.data.response.GetRowSplitResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.MergeUtils;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GetRowHandler extends Handler {

  private static final Log LOG = LogFactory.getLog(GetRowHandler.class);

  public GetRowHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    GetRowSplitResponse response = new GetRowSplitResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    GetRowRequest getRowRequest = (GetRowRequest) userRequest;
    MapResponseCache cache = (MapResponseCache) responseCache;

    // Merge the sub-response
    List<ServerRow> serverRows = new ArrayList<>(cache.expectedResponseNum);
    for (Response response : cache.getResponses().values()) {
      serverRows.add(((GetRowSplitResponse) (response.getData())).getRowSplit());
    }
    Vector vector = MergeUtils
        .combineServerRowSplits(serverRows, getRowRequest.getMatrixId(), getRowRequest.getRowId());

    // Set matrix/row information
    vector.setMatrixId(getRowRequest.getMatrixId());
    vector.setRowId(getRowRequest.getRowId());

    // Set result
    finalResult.set(vector);
  }
}
