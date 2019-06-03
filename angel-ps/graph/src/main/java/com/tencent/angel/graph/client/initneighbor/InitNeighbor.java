package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;

import java.util.Map;

public class InitNeighbor extends UpdateFunc {
    /**
     * Create a new UpdateParam
     */
    public InitNeighbor(UpdateParam param) {
        super(param);
    }

    public InitNeighbor() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        PartInitNeighborParam param = (PartInitNeighborParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        RowBasedPartition part = (RowBasedPartition)matrix.getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

        for (Map.Entry<Long, Node> entry : param.getNodeIdToNode().entrySet()) {
            row.set(entry.getKey(), entry.getValue());
        }
    }
}
