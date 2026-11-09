package com.tencent.angel.spark.ml.ann_core.psf.updateSearchResult;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.matrix.MatrixMeta;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import com.tencent.angel.psagent.matrix.transport.router.KeyValuePart;
import com.tencent.angel.psagent.matrix.transport.router.RouterUtils;
import com.tencent.angel.spark.ml.ann_core.NearestNeighborsIElement;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class UpdateSearchResultParam extends UpdateParam {
    private long[] nodes;
    private List<PartitionUpdateParam> partParams;
    private NearestNeighborsIElement[] updates;
    private int dim;

    public UpdateSearchResultParam(int matrixId, long[] nodes,
                                   NearestNeighborsIElement[] updates, int dim) {
        super(matrixId);
        this.nodes = nodes;
        this.partParams = new ArrayList<>();
        this.updates = updates;
        this.dim = dim;
    }

    public UpdateSearchResultParam() {
        this(-1, null, null, 0);
    }

    public long[] getNodes() {
        return nodes;
    }

    public List<PartitionUpdateParam> getPartParams() {
        return partParams;
    }

    public NearestNeighborsIElement[] getUpdates() {
        return updates;
    }

    @Override
    public List<PartitionUpdateParam> split() {
        MatrixMeta meta = PSAgentContext.get().getMatrixMetaManager().getMatrixMeta(matrixId);
        PartitionKey[] parts = meta.getPartitionKeys();

        KeyValuePart[] splits = RouterUtils.split(meta, 0, nodes, updates, false);
        assert parts.length == splits.length;

        List<PartitionUpdateParam> partParams = new ArrayList<>(parts.length);

        for (int i = 0; i < parts.length; i += 1) {
            if (splits[i] != null && splits[i].size() > 0) {
                partParams.add(new UpdateSearchResultPartParam(matrixId, parts[i], splits[i], this.dim));
            }
        }

        return partParams;
    }
}
