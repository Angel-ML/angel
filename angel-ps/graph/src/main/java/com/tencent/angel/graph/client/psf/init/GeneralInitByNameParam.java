package com.tencent.angel.graph.client.psf.init;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.psf.update.GeneralPartUpdateByNameParam;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class GeneralInitByNameParam extends GeneralInitParam {

  private String name;

  public GeneralInitByNameParam(int matrixId, long[] nodeIds, IElement[] features, String name) {
    super(matrixId, nodeIds, features);
    this.name = name;
  }

  public GeneralInitByNameParam() {
    this(-1, null, null, "");
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    PartitionKey[] parts = meta.getPartitionKeys();

    KeyValuePart[] splits = RouterUtils.split(meta, 0, getNodeIds(), getFeatures());
    assert parts.length == splits.length;

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new GeneralPartUpdateByNameParam(matrixId, parts[i], splits[i], name));
      }
    }

    return partParams;
  }
}