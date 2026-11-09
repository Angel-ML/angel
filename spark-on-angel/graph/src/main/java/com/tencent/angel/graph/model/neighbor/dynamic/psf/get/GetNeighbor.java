package com.tencent.angel.graph.model.neighbor.dynamic.psf.get;

import com.tencent.angel.graph.common.conf.Constents;
import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.common.psf.result.GetLongsResult;
import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import com.twitter.chill.ScalaKryoInstantiator;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * Sample the neighbor
 */
public class GetNeighbor extends GetFunc {

  public GetNeighbor(LongKeysGetParam param) {
    super(param);
  }

  public GetNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GeneralPartGetParam param = (GeneralPartGetParam) partParam;
    KeyPart keyPart = param.getIndicesPart();

    // Long type node id
    long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    // Get data
    byte[][] data = new byte[nodeIds.length][];
    for (int i = 0; i < nodeIds.length; i++) {
      DynamicNeighborElement ele = (DynamicNeighborElement) row.get(nodeIds[i]);
      if (ele != null) {
        data[i] = ele.getData();
      } else {
        data[i] = null;
      }
    }
    return new PartGetNeighborResult(nodeIds, data);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGetNeighborResult) result).getNodes().length;
    }

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGetNeighborResult partResult = (PartGetNeighborResult) result;
      long[] nodeIds = partResult.getNodes();
      byte[][] neighbors = partResult.getNeighbors();
      for (int i = 0; i < nodeIds.length; i++) {
        if (neighbors[i] == null || neighbors[i].length == 0) {
          nodeIdToNeighbors.put(nodeIds[i], Constents.emptyLongs);
        } else {
          nodeIdToNeighbors.put(nodeIds[i], ScalaKryoInstantiator.defaultPool().fromBytes(neighbors[i], long[].class));
        }
      }
    }

    return new GetLongsResult(nodeIdToNeighbors);
  }
}