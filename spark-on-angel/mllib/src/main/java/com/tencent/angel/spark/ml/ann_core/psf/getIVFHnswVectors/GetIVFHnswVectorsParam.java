package com.tencent.angel.spark.ml.ann_core.psf.getIVFHnswVectors;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.get.base.GeneralPartGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import java.util.ArrayList;
import java.util.List;

public class GetIVFHnswVectorsParam extends GetParam {
    private long[] nodes;

    public GetIVFHnswVectorsParam(int matrixId, long[] nodes) {
        super(matrixId);
        this.nodes = nodes;
    }

    @Override
    public List<PartitionGetParam> split() {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        KeyPart[] nodeIdsParts = RouterUtils.split(meta, 0, nodes, false);

        List<PartitionGetParam> partParams = new ArrayList<>(parts.length);
        assert parts.length == nodeIdsParts.length;
        for (int i = 0; i < parts.length; i++) {
            if (nodeIdsParts[i] != null && nodeIdsParts[i].size() > 0) {
                partParams.add(new GeneralPartGetParam(matrixId, parts[i], nodeIdsParts[i]));
            }
        }
        return partParams;
    }

}
