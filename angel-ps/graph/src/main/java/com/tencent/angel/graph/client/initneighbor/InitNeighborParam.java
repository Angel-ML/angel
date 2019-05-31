package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;

import java.util.List;

public class InitNeighborParam extends UpdateParam {
    public InitNeighborParam(int matrixId) {
        super(matrixId);
    }

    @Override
    public List<PartitionUpdateParam> split() {
        return null;
    }
}
