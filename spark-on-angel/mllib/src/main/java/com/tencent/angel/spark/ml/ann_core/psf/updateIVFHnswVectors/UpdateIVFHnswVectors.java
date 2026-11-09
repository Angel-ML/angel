package com.tencent.angel.spark.ml.ann_core.psf.updateIVFHnswVectors;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;
import com.tencent.angel.spark.ml.ann_core.ANNVector;
import com.tencent.angel.spark.ml.ann_core.IVFHnswVector;

public class UpdateIVFHnswVectors extends UpdateFunc {

    public UpdateIVFHnswVectors(int matrixId, long[] nodes, IVFHnswVector[] updates) {
        this(new UpdateIVFHnswVectorsParam(matrixId, nodes, updates));
    }

    public UpdateIVFHnswVectors(UpdateIVFHnswVectorsParam param) {
        super(param);
    }

    public UpdateIVFHnswVectors() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        GeneralPartUpdateParam param = (GeneralPartUpdateParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
        long[] nodes = split.getKeys();
        IElement[] vectors = split.getValues();
        row.startWrite();
        try {
            for (int i = 0; i < nodes.length; i += 1) {
                row.set(nodes[i], vectors[i]);
            }
        } finally {
            row.endWrite();
        }
    }
}
