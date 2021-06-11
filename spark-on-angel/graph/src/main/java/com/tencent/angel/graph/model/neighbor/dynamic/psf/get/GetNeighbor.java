package com.tencent.angel.graph.model.neighbor.dynamic.psf.get;

import com.tencent.angel.graph.common.conf.Constents;
import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.common.psf.result.GetLongsResult;
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.graph.model.neighbor.dynamic.DynamicNeighborElement;
import com.tencent.angel.graph.utils.GeneralGetUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
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
    return GeneralGetUtils.partitionGet(psContext, partParam);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGeneralGetResult) result).getNodeIds().length;
    }

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGeneralGetResult partResult = (PartGeneralGetResult) result;
      long[] nodeIds = partResult.getNodeIds();
      IElement[] neighbors = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        if (neighbors[i] != null) {
          byte[] nbrs = ((DynamicNeighborElement) neighbors[i]).getData();
          nodeIdToNeighbors.put(nodeIds[i], ScalaKryoInstantiator.defaultPool().fromBytes(nbrs, long[].class));
        } else {
          nodeIdToNeighbors.put(nodeIds[i], Constents.emptyLongs);
        }
      }
    }

    return new GetLongsResult(nodeIdToNeighbors);
  }
}