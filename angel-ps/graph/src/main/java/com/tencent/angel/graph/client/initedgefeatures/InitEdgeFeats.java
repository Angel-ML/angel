package com.tencent.angel.graph.client.initedgefeatures;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

public class InitEdgeFeats extends UpdateFunc {

  public InitEdgeFeats(InitEdgeFeatsParam param) {
    super(param);
  }

  public InitEdgeFeats() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    InitEdgeFeatsPartParam param = (InitEdgeFeatsPartParam) partParam;
    ServerLongAnyRow row = (ServerLongAnyRow) (psContext.getMatrixStorageManager()
        .getRow(param.getPartKey(), 0));

    long[] keys = param.getKeys();
    long[][] neighborArrays = param.getNeighborArrays();
    IntFloatVector[][] edgeFeatureArrays = param.getFeatArrays();
    int[][] neighborTypes = param.getTypeArrays();

    row.startWrite();
    try {
      for (int i = 0; i < keys.length; i++) {
        Node node = (Node) row.get(keys[i]);
        if (node == null) {
          node = new Node();
          row.set(keys[i], node);
        }

        node.setNeighbors(neighborArrays[i]);
        node.setEdgeFeatures(edgeFeatureArrays[i]);
        if (neighborTypes != null) {
          node.setTypes(neighborTypes[i]);
        }

      }
    } finally {
      row.endWrite();
    }

  }
}
