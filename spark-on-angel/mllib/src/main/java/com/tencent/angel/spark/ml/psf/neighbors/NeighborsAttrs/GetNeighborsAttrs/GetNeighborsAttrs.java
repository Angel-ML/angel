package com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.GetNeighborsAttrs;

import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs.NeighborsAttrsElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * get Node's NegibhorAttrTagElement data
 */
public class GetNeighborsAttrs extends GetFunc {

  public GetNeighborsAttrs(GetParam param) {
    super(param);
  }

  public GetNeighborsAttrs() {
    this(null);
  }

  /**
   * get Result according to ps matrix's partition
   *
   * @param partParam the partition parameter
   */
  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return NeighborsAttrsGetUtils.partitionGet(psContext, partParam);
  }

  /**
   * merge result
   *
   * @param partResults the partition results
   */
  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    NeighborsAttrsElement emp = null;
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGeneralGetResult) result).getNodeIds().length;
    }

    Long2ObjectOpenHashMap<NeighborsAttrsElement> nodeIdToNeighbors =
        new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGeneralGetResult partResult = (PartGeneralGetResult) result;
      long[] nodeIds = partResult.getNodeIds();
      IElement[] neighbors = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        if (neighbors[i] != null) {
          nodeIdToNeighbors.put(nodeIds[i], ((NeighborsAttrsElement) neighbors[i]));
        } else {
          nodeIdToNeighbors.put(nodeIds[i], emp);
        }
      }
    }

    return new GetNeighborsAttrsResult(nodeIdToNeighbors);
  }

}
