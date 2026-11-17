package com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.GetNeighborsAttrs;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.model.general.get.PartGeneralGetResult;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import com.tencent.angel.spark.ml.psf.neighbors.NeighborsAttrs.InitNeighborsAttrs.NeighborsAttrsElement;

public class NeighborsAttrsGetUtils {

  public static PartitionGetResult partitionGet(PSContext psContext, PartitionGetParam partParam) {
    GeneralPartGetParam param = (GeneralPartGetParam) partParam;
    KeyPart keyPart = param.getIndicesPart();

    // Long type node id
    long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

    // Get data
    IElement[] data = new IElement[nodeIds.length];
    for (int i = 0; i < nodeIds.length; i++) {
      NeighborsAttrsElement elem = (NeighborsAttrsElement) (row.get(nodeIds[i]));
      if (elem == null) {
        data[i] = new NeighborsAttrsElement();
      } else {
        data[i] = elem;
      }
    }

    MatrixMeta meta = psContext.getMatrixMetaManager().getMatrixMeta(param.getMatrixId());

    try {
      return new PartGeneralGetResult(meta.getValueClass(), nodeIds, data);
    } catch (ClassNotFoundException e) {
      throw new AngelException("Can not get value class ");
    }
  }
}
