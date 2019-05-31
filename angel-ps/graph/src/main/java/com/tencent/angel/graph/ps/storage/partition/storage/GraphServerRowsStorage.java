package com.tencent.angel.graph.ps.storage.partition.storage;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.RowType;
import com.tencent.angel.ps.storage.partition.storage.DenseServerRowsStorage;
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
        int rowStart = partKey.getStartRow();
        int rowEnd = partKey.getEndRow();
        long startCol = partKey.getStartCol();
        long endCol = partKey.getEndCol();

        int elementNum = partKey.getIndexNum();
        if (elementNum <= 0) {
            elementNum = (int) ((endCol - startCol) * estSparsity);
        }
        for (int rowIndex = rowStart; rowIndex < rowEnd; rowIndex++) {
            // TODO: change to graph server row
            ServerRow row = ServerRowFactory
                    .createServerRow(rowIndex, rowType, startCol, endCol, elementNum, valueClass);
            row.init();
            putRow(rowIndex, row);
        }
    }

}
