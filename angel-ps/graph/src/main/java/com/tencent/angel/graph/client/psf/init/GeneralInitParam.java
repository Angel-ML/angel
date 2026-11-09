package com.tencent.angel.graph.client.psf.init;

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

public class GeneralInitParam extends UpdateParam {

  /**
   * Node id to features map
   */
  protected long[] nodeIds;

  protected IElement[] features;

  public GeneralInitParam(int matrixId, long[] nodeIds, IElement[] features) {
    super(matrixId);
    this.nodeIds = nodeIds;
    this.features = features;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] splits = RouterUtils.split(meta, 0, nodeIds, features);
    assert parts.length == splits.length;

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new GeneralPartUpdateParam(matrixId, parts[i], splits[i]));
      }
    }

    return partParams;
  }
}
