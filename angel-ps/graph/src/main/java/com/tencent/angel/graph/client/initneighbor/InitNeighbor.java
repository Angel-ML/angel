package com.tencent.angel.graph.client.initneighbor;

import com.tencent.angel.graph.data.Edge;
import com.tencent.angel.graph.data.Node;
import com.tencent.angel.graph.data.NodeEdgesPair;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;

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
        RowBasedPartition part = (RowBasedPartition) matrix.getPartition(partParam.getPartKey().getPartitionId());
        GraphServerRow row = (GraphServerRow) part.getRow(0);

        for (NodeEdgesPair nodeEdgesPair : param.getNodeEdgesPairs()) {
            Node node = nodeEdgesPair.getNode();
            row.addNode(node);

            for (Edge edge : nodeEdgesPair.getEdges()) {
                row.addEdge(edge);
            }
        }
    }
}
