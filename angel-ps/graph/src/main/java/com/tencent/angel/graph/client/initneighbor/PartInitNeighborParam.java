package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;

public class PartInitNeighborParam extends PartitionUpdateParam {
    private int startIndex;
    private int endIndex;

    public PartInitNeighborParam(int matrixId, PartitionKey partKey,
                                 int startIndex, int endIndex) {
        super(matrixId, partKey);
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }
}
