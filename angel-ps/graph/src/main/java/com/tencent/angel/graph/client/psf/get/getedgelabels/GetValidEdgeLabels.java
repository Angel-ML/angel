package com.tencent.angel.graph.client.psf.get.getedgelabels;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetIElementAttrsResult;
import com.tencent.angel.graph.client.psf.get.utils.GetNodeAttrsParam;
import com.tencent.angel.graph.client.psf.get.utils.PartGetIElementAttrsResult;
import com.tencent.angel.graph.data.EdgeLabel;
import com.tencent.angel.graph.data.GraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class GetValidEdgeLabels extends GetFunc {

  /**
   * Create a get node feats func
   *
   * @param param parameter of get udf
   */
  public GetValidEdgeLabels(GetNodeAttrsParam param) {
    super(param);
  }

  public GetValidEdgeLabels() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GeneralPartGetParam param = (GeneralPartGetParam) partParam;
    KeyPart keyPart = param.getIndicesPart();

    switch (keyPart.getKeyType()) {
      case LONG: {
        // Long type node id
        long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        Long2ObjectOpenHashMap<IElement> nodeIdToEdgeLabels =
            new Long2ObjectOpenHashMap<>(nodeIds.length);
        for (long nodeId : nodeIds) {
          if (row.get(nodeId) == null) {
            // If node not exist, just skip
            continue;
          }
          long[] edges = ((GraphNode) (row.get(nodeId))).getValidEdges();
          int[] labels = ((GraphNode) (row.get(nodeId))).getValidEdgeLabels();
          if (edges != null) {
            nodeIdToEdgeLabels.put(nodeId, new EdgeLabel(edges, labels));
          }
        }
        return new PartGetIElementAttrsResult(param.getPartKey().getPartitionId(),
            nodeIdToEdgeLabels);
      }

      default: {
        // TODO: support String, Int, and Any type node id
        throw new InvalidParameterException("Unsupport index type " + keyPart.getKeyType());
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<IElement> nodeIdToEdgeLabels =
        new Long2ObjectOpenHashMap<>(((GetNodeAttrsParam) param).getNodeIds().length);
    for (PartitionGetResult partitionGetResult : partResults) {
      Long2ObjectOpenHashMap<IElement> partNodeIdToTypes =
          ((PartGetIElementAttrsResult) partitionGetResult).getNodeIdToContents();
      if (partNodeIdToTypes != null) {
        nodeIdToEdgeLabels.putAll(partNodeIdToTypes);
      }
    }
    return new GetIElementAttrsResult(nodeIdToEdgeLabels);
  }
}
