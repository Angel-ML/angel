package com.tencent.angel.graph.embedding.node2vec;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.longs.Long2ObjectMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class InitAliasTable extends UpdateFunc {
    private static final Logger LOG = LoggerFactory.getLogger(InitAliasTable.class);

    /**
     * Create a new UpdateParam
     */
    public InitAliasTable(InitAliasTableParam param) {
        super(param);
    }

    public InitAliasTable() {
        this(null);
    }

    @Override
    public void partitionUpdate(PartitionUpdateParam partParam) {
        InitAliasTablePartParam param = (InitAliasTablePartParam) partParam;
        ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
        RowBasedPartition part = (RowBasedPartition) matrix
                .getPartition(partParam.getPartKey().getPartitionId());
        ServerLongAnyRow row = (ServerLongAnyRow) part.getRow(0);

        ObjectIterator<Long2ObjectMap.Entry<AliasElement>> iter = param.getNodeId2Neighbors().long2ObjectEntrySet().iterator();

        row.startWrite();
        try {
            while (iter.hasNext()) {
                Long2ObjectMap.Entry<AliasElement> entry = iter.next();
                AliasElement element = entry.getValue();
                if (element == null) {
                    row.set(entry.getLongKey(), null);
                } else {
                    row.set(entry.getLongKey(), new AliasElement(element.getNeighborIds(), element.getAccept(), element.getAlias()));
                }
            }
        } finally {
            row.endWrite();
        }
    }
}
