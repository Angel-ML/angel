package com.tencent.angel.graph.model.neighbor.dynamicsimple.psf;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;

import java.util.ArrayList;
import java.util.List;

public class InitNeighborsParam extends UpdateParam {
    private final long[] nodeIds;
    private final int[] count;
    private final boolean isWeighted;

    public InitNeighborsParam(int matrixId, long[] nodeIds, int[] count, boolean isWeighted) {
        super(matrixId);
        this.nodeIds = nodeIds;
        this.count = count;
        this.isWeighted = isWeighted;
    }

    public InitNeighborsParam() {
        this(-1, null, null, false);
    }

    public long[] getNodeIds() {
        return nodeIds;
    }

    public int[] getCount() {
        return count;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        KeyValuePart[] nodeIdsParts = RouterUtils.split(meta, 0, nodeIds, count, false);
        List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
                partParams.add(new PartInitNeighborParam(matrixId, parts[i], nodeIdsParts[i], isWeighted));
            }
        }
        return partParams;
    }
}
