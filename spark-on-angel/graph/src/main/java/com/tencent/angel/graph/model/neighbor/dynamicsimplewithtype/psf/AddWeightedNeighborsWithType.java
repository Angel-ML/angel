package com.tencent.angel.graph.model.neighbor.dynamicsimplewithtype.psf;

import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.psf.AddNeighborsParam;
import com.tencent.angel.graph.model.neighbor.dynamicsimple.psf.PartAddNeighborsParam;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.ps.storage.vector.element.LongIntIntFloatArrayPairElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;

public class AddWeightedNeighborsWithType extends UpdateFunc {

    public AddWeightedNeighborsWithType(AddNeighborsParam param) {
        super(param);
    }

    public AddWeightedNeighborsWithType() {
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
                LongIntIntFloatArrayPairElement data = (LongIntIntFloatArrayPairElement) neighbors[i];
                ele.add(data.getData(), data.getTypes(), data.getIndptr(), data.getWeights(), isAttempt);
            }
        } finally {
            row.endWrite();
        }
    }
}
