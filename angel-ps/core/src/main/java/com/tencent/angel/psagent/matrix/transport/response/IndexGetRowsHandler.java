package com.tencent.angel.psagent.matrix.transport.response;

import com.tencent.angel.ml.math2.vector.Vector;
import com.tencent.angel.ps.server.data.handler.GetRowsHandler;
import com.tencent.angel.ps.server.data.request.IndexPartGetRowsRequest;
import com.tencent.angel.ps.server.data.request.Request;
import com.tencent.angel.ps.server.data.response.IndexPartGetRowsResponse;
import com.tencent.angel.ps.server.data.response.Response;
import com.tencent.angel.ps.server.data.response.ResponseData;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.FutureResult;
import com.tencent.angel.psagent.matrix.transport.adapter.IndexGetRowsRequest;
import com.tencent.angel.psagent.matrix.transport.adapter.MergeUtils;
import com.tencent.angel.psagent.matrix.transport.adapter.UserRequest;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.ValuePart;
import io.netty.buffer.ByteBuf;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class IndexGetRowsHandler extends Handler {
  private static final Log LOG = LogFactory.getLog(GetRowsHandler.class);

  public IndexGetRowsHandler(PSAgentContext context) {
    super(context);
  }

  @Override
  public ResponseData parseResponse(ByteBuf in) {
    IndexPartGetRowsResponse response = new IndexPartGetRowsResponse();
    response.deserialize(in);
    return response;
  }

  @Override
  public void handle(FutureResult finalResult, UserRequest userRequest,
      ResponseCache responseCache) {
    IndexGetRowsRequest indexGetRowsRequest = (IndexGetRowsRequest) userRequest;
    MapResponseCache cache = (MapResponseCache) responseCache;
    ConcurrentHashMap<Request, Response> responses = cache.getResponses();

    // Merge
    Vector[] result = merge(indexGetRowsRequest, responses);

    // Set final result
    finalResult.set(result);
  }

  private Vector[] merge(IndexGetRowsRequest indexGetRowsRequest, ConcurrentHashMap<Request, Response> responses) {
    int[] rowIds = indexGetRowsRequest.getRowIds();
    int matrixId = indexGetRowsRequest.getMatrixId();
    int keyNum = indexGetRowsRequest.size();
    int responseNum = responses.size();
    KeyPart[] keyParts = new KeyPart[responseNum];
    ValuePart[][] valueParts = new ValuePart[rowIds.length][];
    for(int i = 0; i < valueParts.length; i++) {
      valueParts[i] = new ValuePart[responseNum];
    }

    int index = 0;
    for(Entry<Request, Response> entry : responses.entrySet()) {
      keyParts[index] = ((IndexPartGetRowsRequest) (entry.getKey().getData())).getKeyPart();
      ValuePart[] subParts = ((IndexPartGetRowsResponse) (entry.getValue().getData())).getValueParts();
      for(int i = 0; i < subParts.length; i++) {
        valueParts[i][index] = subParts[i];
      }
      index++;
    }

    Vector[] vectors = new Vector[rowIds.length];
    for(int i = 0; i < vectors.length; i++) {
      vectors[i] = MergeUtils.combineIndexRowSplits(matrixId, rowIds[i], keyNum, keyParts, valueParts[i]);
      vectors[i].setMatrixId(matrixId);
      vectors[i].setRowId(rowIds[i]);
    }

    return vectors;
  }
}
