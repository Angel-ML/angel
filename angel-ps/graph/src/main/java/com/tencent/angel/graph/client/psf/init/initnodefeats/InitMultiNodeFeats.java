package com.tencent.angel.graph.client.psf.init.initnodefeats;

import com.tencent.angel.graph.client.psf.init.GeneralInitParam;
import com.tencent.angel.graph.data.Feature;
import com.tencent.angel.graph.data.MultiGraphNode;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

public class InitMultiNodeFeats extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public InitMultiNodeFeats(GeneralInitParam param) {
    super(param);
  }

  public InitMultiNodeFeats() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    GeneralPartUpdateParam initParam = (GeneralPartUpdateParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, initParam);

    // Get node ids and features
    ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) initParam.getKeyValuePart();
    long[] nodeIds = split.getKeys();
    IElement[] features = split.getValues();

    row.startWrite();
    try {
      for (int i = 0; i < nodeIds.length; i++) {
        MultiGraphNode graphNode = (MultiGraphNode) row.get(nodeIds[i]);
        if (graphNode == null) {
          graphNode = new MultiGraphNode();
          row.set(nodeIds[i], graphNode);
        }
        graphNode.setFeats(((Feature) features[i]).getFeatures());
      }
    } finally {
      row.endWrite();
    }
  }
}