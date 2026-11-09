package com.tencent.angel.spark.ml.ann_core.psf.updateIVFHnswVectors;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.GeneralPartUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import com.tencent.angel.spark.ml.ann_core.ANNVector;
import com.tencent.angel.spark.ml.ann_core.IVFHnswVector;
import java.util.ArrayList;
import java.util.List;

public class UpdateIVFHnswVectorsParam extends UpdateParam {
    private long[] nodes;
    private List<PartitionUpdateParam> partParams;
    private IVFHnswVector[] updates;

    public UpdateIVFHnswVectorsParam(int matrixId, long[] nodes, IVFHnswVector[] updates) {
        super(matrixId);
        this.nodes = nodes;
        this.partParams = new ArrayList<>();
        this.updates = updates;
    }

    public UpdateIVFHnswVectorsParam() {
        this(-1, null, null);
    }

    public long[] getNodes() {
        return nodes;
    }

    public List<PartitionUpdateParam> getPartParams() {
        return partParams;
    }

    public IVFHnswVector[] getUpdates() {
        return updates;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        KeyValuePart[] splits = RouterUtils.split(meta, 0, nodes, updates, false);
        assert parts.length == splits.length;

        List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);
        for (int i = 0; i < parts.length; i++) {
            if (splits[i] != null && splits[i].size() > 0) {
                partParams.add(new GeneralPartUpdateParam(matrixId, parts[i], splits[i]));
            }
        }

        return partParams;
    }

}
