package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyIntValuePartOp;

public class InitNeighbors extends UpdateFunc {

    public InitNeighbors(InitNeighborsParam param) {
        super(param);
    }

    public InitNeighbors() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        PartInitNeighborParam initParam = (PartInitNeighborParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, initParam);
        ILongKeyIntValuePartOp split = (ILongKeyIntValuePartOp) initParam.getIndicesPart();
        long[] nodeIds = split.getKeys();
        int[] counts = split.getValues();
        boolean isWeighted = initParam.getIsWeighted();

        row.startWrite();
        try {
            for(int i = 0; i < nodeIds.length; i++) {
                if (isWeighted) {
                    row.set(nodeIds[i], new DynamicSimpleNeighborElement(new long[counts[i]], new float[counts[i]], 0));
                } else {
                    row.set(nodeIds[i], new DynamicSimpleNeighborElement(new long[counts[i]], 0));
                }
            }
        } finally {
            row.endWrite();
        }
    }
}
