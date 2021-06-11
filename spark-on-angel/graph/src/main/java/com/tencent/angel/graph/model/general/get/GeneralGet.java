package com.tencent.angel.graph.model.general.get;

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.common.psf.result.GetElementResult;
import com.tencent.angel.graph.utils.GeneralGetUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class GeneralGet extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public GeneralGet(LongKeysGetParam param) {
    super(param);
  }

  public GeneralGet() { this(null); }

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

    Long2ObjectOpenHashMap nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(resultSize);
    for (PartitionGetResult result : partResults) {
      PartGeneralGetResult getResult = (PartGeneralGetResult) result;
      long[] nodeIds = getResult.getNodeIds();
      IElement[] objs = getResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        nodeIdToNeighbors.put(nodeIds[i], objs[i]);
      }
    }
    return new GetElementResult(nodeIdToNeighbors);
  }
}
