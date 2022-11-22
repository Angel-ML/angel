package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongFloatArrayPairElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

public class AddWeightedNeighbors extends UpdateFunc {

    public AddWeightedNeighbors(AddNeighborsParam param) {
        super(param);
    }

    public AddWeightedNeighbors() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        PartAddNeighborsParam updateParam = (PartAddNeighborsParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, updateParam);
        ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) updateParam.getKeyValuePart();
        long[] nodeIds = split.getKeys();
        IElement[] neighbors = split.getValues();
        boolean isTaskAttempt = updateParam.getIsAttempt();
        int tryCount = psContext.getTryCount();
        boolean isAttempt = isTaskAttempt || tryCount > 1;

        int lockTime = psContext.getConf().getInt(AngelConf.ANGEL_PS_MAX_LOCK_WAITTIME_MS, AngelConf.DEFAULT_ANGEL_PS_MAX_LOCK_WAITTIME_MS);
        row.startWrite(lockTime);
        try {
            for(int i = 0; i < nodeIds.length; i++) {
                DynamicSimpleNeighborElement ele = (DynamicSimpleNeighborElement) row.get(nodeIds[i]);
                LongFloatArrayPairElement data = (LongFloatArrayPairElement) neighbors[i];
                ele.add(data.getData(), data.getWeights(), isAttempt);
            }
        } finally {
            row.endWrite();
        }
    }
}

