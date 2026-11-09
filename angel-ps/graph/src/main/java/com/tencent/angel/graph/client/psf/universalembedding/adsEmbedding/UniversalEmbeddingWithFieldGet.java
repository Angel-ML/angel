package com.tencent.angel.graph.client.psf.universalembedding.adsEmbedding;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetMapAttrsResult;
import com.tencent.angel.graph.client.psf.get.utils.PartGetMapAttrsResult;
import com.tencent.angel.graph.data.UniversalEmbeddingNode;
import com.tencent.angel.graph.data.UniversalEmbeddingNodeWithField;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyIntValuePartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class UniversalEmbeddingWithFieldGet extends GetFunc {

  public UniversalEmbeddingWithFieldGet(UniversalEmbeddingWithFieldGetParam param) {
    super(param);
  }

  public UniversalEmbeddingWithFieldGet() {
    super(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartUniversalEmbeddingWithFieldGetParam param = (PartUniversalEmbeddingWithFieldGetParam) partParam;
    KeyValuePart keyValuePart = param.getKeyValuePart();

    switch (keyValuePart.getKeyValueType()) {
      case T_INT_SPARSE_LONGKEY: {
        // Long type node id
        long[] nodeIds = ((ILongKeyIntValuePartOp) keyValuePart).getKeys();
        int[] fieldIds = ((ILongKeyIntValuePartOp) keyValuePart).getValues();
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        Long2ObjectOpenHashMap<Map<Integer, float[]>> nodeIdToEmbeddings =
            new Long2ObjectOpenHashMap<>(nodeIds.length);
        for (int i = 0; i < nodeIds.length; i++) {
          long nodeId = nodeIds[i];
          int fieldId = fieldIds[i];
          if (row.get(nodeId) == null) {
            // If node not exist, just skip
            continue;
          }
          UniversalEmbeddingNodeWithField embeddingNode = (UniversalEmbeddingNodeWithField) row
              .get(nodeId);
          if (embeddingNode.getFieldId2Emb() != null) {
            UniversalEmbeddingNode embedding = embeddingNode.getFieldId2Emb()
                .get(fieldId);
            if (embedding != null && embedding.getEmbeddings() != null
                && fieldId != -1) {
              float[] emb = embedding.getEmbeddings();
              if (emb != null) {
                Map<Integer, float[]> fieldId2Embedding = nodeIdToEmbeddings.get(nodeId);
                if (fieldId2Embedding == null) {
                  fieldId2Embedding = new HashMap<>();
                  nodeIdToEmbeddings.put(nodeId, fieldId2Embedding);
                }
                fieldId2Embedding.put(fieldId, emb);
              }
            }
          }
        }
        return new PartGetMapAttrsResult(param.getPartKey().getPartitionId(),
            nodeIdToEmbeddings);
      }

      default: {
        // TODO: support String, Int, and Any type node id
        throw new InvalidParameterException("Unsupported index type " + keyValuePart.getKeyValueType().name());
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Map<Integer, float[]>> nodeIdToEmbeddings =
        new Long2ObjectOpenHashMap<>(
            ((UniversalEmbeddingWithFieldGetParam) param).getNodeIds().length);

    for (PartitionGetResult part : partResults) {
      Long2ObjectOpenHashMap<Map<Integer, float[]>> partNodeIdToEmbeddings =
          ((PartGetMapAttrsResult) part).getNodeIdToContents();
      if (partNodeIdToEmbeddings != null) {
        nodeIdToEmbeddings.putAll(partNodeIdToEmbeddings);
      }
    }

    return new GetMapAttrsResult(nodeIdToEmbeddings);
  }
}
