package com.tencent.angel.spark.ml.psf.neighbors;

import com.tencent.angel.graph.common.psf.result.GetElementResult;
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.graph.utils.GeneralGetUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * get Node's NegibhorAttrTagElement data
 */
public class GetNeighborAttrTag extends GetFunc {

  public GetNeighborAttrTag(GetParam param) {
    super(param);
  }

  public GetNeighborAttrTag() {
    this(null);
  }

  /**
   * get Result according to ps matrix's partition
   *
   * @param partParam the partition parameter
   */
  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return GeneralGetUtils.partitionGet(psContext, partParam);
  }

  /**
   * merge result
   *
   * @param partResults the partition results
   */
  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGeneralGetResult) result).getNodeIds().length;
    }

    Long2ObjectOpenHashMap<NeighborsAttrTagElement> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(
        resultSize);

    return new GetElementResult(nodeIdToNeighbors);
  }

}
