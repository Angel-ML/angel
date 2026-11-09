package com.tencent.angel.graph.model.neighbor.simplewithtype.psf.getType;

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.common.psf.result.GetIntsResult;
import com.tencent.angel.graph.common.psf.result.PartGetIntsResult;
import com.tencent.angel.graph.model.neighbor.simplewithtype.TypeNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * Sample the neighbor
 */
public class GetType extends GetFunc {

  public GetType(LongKeysGetParam param) {
    super(param);
  }

  public GetType() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    GeneralPartGetParam param = (GeneralPartGetParam) partParam;
    KeyPart keyPart = param.getIndicesPart();

    long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
    int[][] data = new int[nodeIds.length][];
    for (int i = 0; i < nodeIds.length; i++) {
      long node = nodeIds[i];
      TypeNeighborElement ele = (TypeNeighborElement) row.get(node);
      if (ele == null) {
        data[i] = new int[0];
      } else {
        data[i] = ele.getTypes();
      }
    }
    return new PartGetIntsResult(nodeIds, data);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGetIntsResult) result).getNodeIds().length;
    }

    Long2ObjectOpenHashMap<int[]> nodeIdToTypes = new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGetIntsResult partResult = (PartGetIntsResult) result;
      long[] nodeIds = partResult.getNodeIds();
      int[][] types = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        nodeIdToTypes.put(nodeIds[i], types[i]);
      }
    }

    return new GetIntsResult(nodeIdToTypes);
  }
}