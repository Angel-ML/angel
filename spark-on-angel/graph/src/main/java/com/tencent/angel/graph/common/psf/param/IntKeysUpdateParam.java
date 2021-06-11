package com.tencent.angel.graph.common.psf.param;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class IntKeysUpdateParam extends UpdateParam {

  /**
   * Node ids
   */
  private final int[] nodeIds;

  /**
   * Serialized neighbor table
   */
  private final IElement[] neighbors;

  public IntKeysUpdateParam(int matrixId, int[] nodeIds, IElement[] neighbors) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.neighbors = neighbors;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, neighbors);
    assert parts.length == splits.length;

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new GeneralPartUpdateParam(matrixId, parts[i], splits[i]));
      }
    }

    return partParams;
  }

  public int[] getNodeIds() {
    return nodeIds;
  }

  public IElement[] getNeighbors() {
    return neighbors;
  }
}
