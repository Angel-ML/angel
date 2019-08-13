package com.tencent.angel.graph.client.initnodefeats3;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitNodeFeats extends UpdateFunc {

  public InitNodeFeats(InitNodeFeatsParam param) {
    super(param);
  }

  public InitNodeFeats() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitNodeFeatsPartParam param = (InitNodeFeatsPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) psContext.getMatrixStorageManager().getRow(param.getPartKey(), 0);

    long[] nodeIds = param.getNodeIds();
    IntFloatVector[] feats = param.getFeats();

    try {
      row.startWrite();
      for (int i = 0; i < nodeIds.length; i++) {
        Node node = (Node) row.get(nodeIds[i]);
        if (node == null) {
          node = new Node();
          row.set(nodeIds[i], node);
        }
        node.setFeats(feats[i]);
        if (nodeIds[i] == 0) {
          IntFloatVector f = feats[i];
          float[] values = f.getStorage().getValues();
          for (int j = 0; j < values.length; j++) {
            System.out.println(values[j] + " ");
          }
          System.out.println();
        }
      }
    } finally {
      row.endWrite();
    }
  }

}
