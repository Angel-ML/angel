package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.server.data.response.GetRowsSplitResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.MergeUtils;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import io.netty.buffer.ByteBuf;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class GetRowsHandler extends Handler {

  public GetRowsHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    GetRowsSplitResponse response = new GetRowsSplitResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    GetRowsRequest getRowsRequest = (GetRowsRequest) userRequest;
    MapResponseCache cache = (MapResponseCache) responseCache;

    // Merge the sub-response
    Vector[] vectors = merge(getRowsRequest.getMatrixId(), getRowsRequest.getRowIds(),
        cache.getResponses().values());

    // Set the result
    finalResult.set(vectors);
  }

  private Vector[] merge(int matrixId, int[] rowIds, Collection<Response> responses) {
    Map<Integer, List<ServerRow>> rowIdToserverRows = new HashMap<>(rowIds.length);
    int valueNum = responses.size();
    for (Response response : responses) {
      ServerRow[] serverRows = ((GetRowsSplitResponse) (response.getData())).getRowsSplit();
      for (int j = 0; j < serverRows.length; j++) {
        int rowId = serverRows[j].getRowId();
        List<ServerRow> rowSplits = rowIdToserverRows.get(rowId);
        if (rowSplits == null) {
          rowSplits = new ArrayList<>(valueNum);
          rowIdToserverRows.put(rowId, rowSplits);
        }
        rowSplits.add(serverRows[j]);
      }
    }

    Vector[] vectors = new Vector[rowIds.length];
    for (int i = 0; i < rowIds.length; i++) {
      vectors[i] = MergeUtils
          .combineServerRowSplits(rowIdToserverRows.get(rowIds[i]), matrixId, rowIds[i]);
      vectors[i].setMatrixId(matrixId);
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }
}
