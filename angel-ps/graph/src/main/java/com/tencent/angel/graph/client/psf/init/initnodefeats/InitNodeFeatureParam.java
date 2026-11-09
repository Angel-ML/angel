package com.tencent.angel.graph.client.psf.init.initnodefeats;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.client.psf.init.GeneralInitParam;
import com.tencent.angel.graph.data.FeatureKeyType;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class InitNodeFeatureParam extends GeneralInitParam {

  FeatureKeyType keyType;

  public InitNodeFeatureParam(int matrixId, long[] nodeIds, IElement[] features, FeatureKeyType keyType) {
    super(matrixId, nodeIds, features);
    this.keyType = keyType;
  }

  public InitNodeFeatureParam() {
    this(-1, null, null, FeatureKeyType.INT);
  }

  public FeatureKeyType getKeyType() {
    return keyType;
  }

  public void setKeyType(FeatureKeyType keyType) {
    this.keyType = keyType;
  }

  @Override
  public List<PartitionUpdateParam> split() {
    MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
    if (meta == null) {
      throw new IllegalArgumentException("MatrixMeta is null for matrixId: " + matrixId);
    }
    PartitionKey[] parts = meta.getPartitionKeys();

    final int ROW_ID = 0;
    KeyValuePart[] splits = RouterUtils.split(meta, ROW_ID, nodeIds, features);
    assert parts.length == splits.length;

    List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
    for (int i = 0; i < parts.length; i++) {
      if (splits[i] != null && splits[i].size() > 0) {
        partParams.add(new PartInitNodeFeatureParam(matrixId, parts[i], splits[i], keyType));
      }
    }

    return partParams;
  }
}
