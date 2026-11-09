package com.tencent.angel.spark.ml.ann_core.psf.updateSearchResult;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.IElement;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyAnyValuePartOp;
import com.tencent.angel.spark.ml.ann_core.NearestNeighborsIElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class UpdateSearchResult extends UpdateFunc {
    public UpdateSearchResult(int matrixId, long[] nodes, NearestNeighborsIElement[] updates, int topK) {
        this(new UpdateSearchResultParam(matrixId, nodes, updates, topK));
    }

    public UpdateSearchResult(UpdateSearchResultParam param) {
        super(param);
    }

    public UpdateSearchResult() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        UpdateSearchResultPartParam param = (UpdateSearchResultPartParam) partParam;
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        int topK = ((UpdateSearchResultPartParam) partParam).getDim();
        ILongKeyAnyValuePartOp split = (ILongKeyAnyValuePartOp) param.getKeyValuePart();
        long[] nodes = split.getKeys();
        IElement[] nearNbrs = split.getValues();
        row.startWrite();
        long startTime = System.currentTimeMillis();
        try {
            for (int i = 0; i < nodes.length; i += 1) {
                if (!row.exist(nodes[i])) {
                    row.set(nodes[i], nearNbrs[i]);
                }
                else {
                    NearestNeighborsIElement old = (NearestNeighborsIElement) row.get(nodes[i]);
                    NearestNeighborsIElement merged = ((NearestNeighborsIElement) nearNbrs[i]).merge(old, topK);
                    row.set(nodes[i], merged);
                }
            }
        } finally {
            row.endWrite();
        }
        long endTime = System.currentTimeMillis();
        //System.out.print("merge and set in partitionUpdate cost: ");
        //System.out.print(endTime - startTime);
        //System.out.println(" ms");
    }
}
