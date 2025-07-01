package com.tencent.angel.graph.client.psf.init.initneighbors;

import com.tencent.angel.graph.client.psf.init.GeneralInitByNameParam;
import com.tencent.angel.graph.client.psf.update.GeneralPartUpdateByNameParam;
import com.tencent.angel.graph.data.LongNeighbor;
import com.tencent.angel.graph.data.MultiGraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

/**
 * Init neighbor table for GraphNode
 */
public class InitNeighborByName extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public InitNeighborByName(GeneralInitByNameParam param) {
    super(param);
  }

  public InitNeighborByName() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    GeneralPartUpdateByNameParam param = (GeneralPartUpdateByNameParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
    ILongKeyAnyValuePartOp keyValuePart = (ILongKeyAnyValuePartOp) param.getKeyValuePart();

    long[] nodeIds = keyValuePart.getKeys();
    IElement[] neighbors = keyValuePart.getValues();
    String name = param.getName();

    row.startWrite();
    try {
      for (int i = 0; i < nodeIds.length; i++) {
        MultiGraphNode graphNode = (MultiGraphNode) row.get(nodeIds[i]);
        if (graphNode == null) {
          graphNode = new MultiGraphNode();
          row.set(nodeIds[i], graphNode);
        }
        graphNode.setNeighbors(name, ((LongNeighbor) neighbors[i]).getNodeIds());
      }
    } finally {
      row.endWrite();
    }
  }
}