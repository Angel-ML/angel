package com.tencent.angel.graph.client.psf.get.getnodefeats;

import com.tencent.angel.exception.InvalidParameterException;
import com.tencent.angel.graph.client.psf.get.utils.GetNodeAttrsParam;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.FeatureKeyType;
import com.tencent.angel.graph.data.MultiGraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.ByteArrayElement;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetMultiNodeFeats extends GetNodeFeats {

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

        Long2ObjectOpenHashMap<ByteArrayElement> nodeId2Feats =
            new Long2ObjectOpenHashMap<>(nodeIds.length);
        FeatureKeyType keyType = null;

        for (long nodeId : nodeIds) {
          if (row.get(nodeId) == null) {
            // If node not exist, just skip
            continue;
          }
          MultiGraphNode node = (MultiGraphNode) row.get(nodeId);
          ByteArrayElement feat = node.getFeats();
          if (feat != null) {
            nodeId2Feats.put(nodeId, feat);
            keyType = node.getKeyType();
          }
        }

        if (keyType == null && nodeId2Feats.isEmpty()) {
          keyType = FeatureKeyType.INT;
        }

        return new PartGetNodeFeatsResult(param.getPartKey().getPartitionId(), nodeId2Feats, keyType);
      }

      default: {
        // TODO: support String, Int, and Any type node id
        throw new InvalidParameterException("Unsupported index type " + keyPart.getKeyType());
      }
    }
  }

}