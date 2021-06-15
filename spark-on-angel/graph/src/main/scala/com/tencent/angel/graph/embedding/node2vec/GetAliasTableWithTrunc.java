package com.tencent.angel.graph.embedding.node2vec;

import com.tencent.angel.graph.common.psf.param.LongKeysGetParam;
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.element.IElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import scala.Tuple3;

/**
 * Sample the neighbor with truncation if needed
 */

public class GetAliasTableWithTrunc extends GetFunc {

  public static final Tuple3 emp = new Tuple3<>(new long[0], new float[0], new int[0]);
  private boolean useTrunc = false;
  private int truncLen = -1;

  public GetAliasTableWithTrunc(LongKeysGetParam param, boolean useTrunc, int truncLen) {
    super(param);
    this.useTrunc = useTrunc;
    this.truncLen = truncLen;
  }

  public GetAliasTableWithTrunc() {
    this(null, false, -1);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return AliasGetUtils.partitionGet(psContext, partParam, useTrunc, truncLen);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      resultSize += ((PartGeneralGetResult) result).getNodeIds().length;
    }

    Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> nodeIdToNeighbors =
        new Long2ObjectOpenHashMap<>(resultSize);

    for (PartitionGetResult result : partResults) {
      PartGeneralGetResult partResult = (PartGeneralGetResult) result;
      long[] nodeIds = partResult.getNodeIds();
      IElement[] data = partResult.getData();
      for (int i = 0; i < nodeIds.length; i++) {
        if (data[i] != null) {
          long[] neighbors = ((AliasElement) data[i]).getNeighborIds();
          float[] accept = ((AliasElement) data[i]).getAccept();
          int[] alias = ((AliasElement) data[i]).getAlias();
          if (neighbors.length > 0 && accept.length > 0 && alias.length > 0) {
            nodeIdToNeighbors.put(nodeIds[i], new Tuple3<>(neighbors, accept, alias));
          } else {
            nodeIdToNeighbors.put(nodeIds[i], emp);
          }
        } else {
          nodeIdToNeighbors.put(nodeIds[i], emp);
        }
      }
    }

    return new PullAliasResult(nodeIdToNeighbors);
  }
}
