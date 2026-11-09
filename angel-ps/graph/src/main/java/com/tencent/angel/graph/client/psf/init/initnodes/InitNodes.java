package com.tencent.angel.graph.client.psf.init.initnodes;

import com.tencent.angel.graph.client.psf.init.GeneralInitParam;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

public class InitNodes extends UpdateFunc {

  /**
   * Create a new UpdateParam
   */
  public InitNodes(GeneralInitParam param) {
    super(param);
  }

  public InitNodes() {
    this(null);
  }

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    GeneralPartUpdateParam initParam = (GeneralPartUpdateParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, initParam);

    // Get nodes and features
    ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) initParam.getKeyValuePart();
    long[] nodeIds = split.getKeys();
    IElement[] featuress = split.getValues();

    row.startWrite();
    try {
      for (int i = 0; i < nodeIds.length; i++) {
        row.set(nodeIds[i], featuress[i]);
      }
    } finally {
      row.endWrite();
    }
  }
}