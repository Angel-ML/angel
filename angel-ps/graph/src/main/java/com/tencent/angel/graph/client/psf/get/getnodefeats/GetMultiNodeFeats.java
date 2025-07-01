package com.tencent.angel.graph.client.psf.get.getnodefeats;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetNodeAttrsParam;
import com.tencent.angel.graph.data.MultiGraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.math2.vector.FloatVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class GetMultiNodeFeats extends GetFunc {

  /**
   * Create a get node feats func
   *
   * @param param parameter of get udf
   */
  public GetMultiNodeFeats(GetNodeAttrsParam param) {
    super(param);
  }

  public GetMultiNodeFeats() {
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

        Long2ObjectOpenHashMap<IntFloatVector> nodeIdToFeats =
                new Long2ObjectOpenHashMap<>(nodeIds.length);
        for (long nodeId : nodeIds) {
          if (row.get(nodeId) == null) {
            // If node not exist, just skip
            continue;
          }
          IntFloatVector feat = ((MultiGraphNode) (row.get(nodeId))).getFeats();
          if (feat != null) {
            nodeIdToFeats.put(nodeId, feat);
          }
        }
        return new PartGetNodeFeatsResult(param.getPartKey().getPartitionId(), nodeIdToFeats);
      }

      default: {
        // TODO: support String, Int, and Any type node id
        throw new InvalidParameterException("Unsupport index type " + keyPart.getKeyType());
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<IntFloatVector> nodeIdToFeats =
            new Long2ObjectOpenHashMap<>(((GetNodeAttrsParam) param).getNodeIds().length);
    for (PartitionGetResult partitionGetResult : partResults) {
      Long2ObjectOpenHashMap<IntFloatVector> partNodeIdToFeats =
              ((PartGetNodeFeatsResult) partitionGetResult).getNodeIdTofeats();
      if (partNodeIdToFeats != null) {
        nodeIdToFeats.putAll(partNodeIdToFeats);
      }
    }
    return new GetNodeFeatsResult(nodeIdToFeats);
  }
}