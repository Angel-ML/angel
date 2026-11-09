package com.tencent.angel.graph.client.initnodefeats4;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class UpdateNodeFeats extends UpdateFunc {

  public UpdateNodeFeats(InitNodeFeatsParam param) {
    super(param);
  }

  public UpdateNodeFeats() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitNodeFeatsPartParam param = (InitNodeFeatsPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager()
        .getRow(param.getPartKey(), 0);

    long[] nodeIds = param.getNodeIds();
    IntFloatVector[] feats = param.getFeats();

    row.startWrite();
    try {
      for (int i = 0; i < nodeIds.length; i++) {
        Node node = (Node) row.get(nodeIds[i]);
        if (node == null) {
          node = new Node();
          row.set(nodeIds[i], node);
        }
        if (node.getFeats() != null) {
          node.setFeats((IntFloatVector) node.getFeats().add(feats[i]));
        } else {
          node.setFeats(feats[i]);
        }
      }
    } finally {
      row.endWrite();
    }
  }
}
