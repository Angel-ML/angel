package com.tencent.angel.graph.ps.storage.partition.storage;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.graph.ps.storage.vector.GraphServerRow;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.partition.storage.DenseServerRowsStorage;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.ServerRow;
import com.tencent.angel.ps.storage.vector.ServerRowFactory;
import com.tencent.angel.ps.storage.vector.element.IElement;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class GraphServerRowsStorage extends DenseServerRowsStorage {

    /**
     * Create new DenseServerRowsStorage
     *
     * @param rowIdOffset row id offset
     * @param rowNum      row number
     */
    public GraphServerRowsStorage(int rowIdOffset, int rowNum) {
        super(rowIdOffset, rowNum);
    }

    @Override
    public void init(
            PartitionKey partKey, RowType rowType, double estSparsity,
            Class<? extends IElement> valueClass) {
        long startCol = partKey.getStartCol();
        long endCol = partKey.getEndCol();

        ServerRow row = new GraphServerRow(startCol, endCol);

        row.init();
        putRow(0, row);
    }

}
