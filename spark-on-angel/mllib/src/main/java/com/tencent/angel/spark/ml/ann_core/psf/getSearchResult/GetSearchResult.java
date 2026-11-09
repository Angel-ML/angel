package com.tencent.angel.spark.ml.ann_core.psf.getSearchResult;

import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.*;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.KeyPart;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import com.tencent.angel.spark.ml.ann_core.NearestNeighborsIElement;
import com.tencent.angel.spark.ml.ann_core.NeighborDistance;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class GetSearchResult extends GetFunc {
    private int matrixId;

    public GetSearchResult(GetSearchResultParam param) {
        super(param);
        matrixId = param.getMatrixId();
    }

    public GetSearchResult() {
        super(null);
    }

    @Override
    public PartitionGetResult partitionGet(PartitionGetParam partParam) {
        GeneralPartGetParam param = (GeneralPartGetParam) partParam;
        KeyPart keyPart = param.getIndicesPart();
        long[] nodeIds = ((ILongKeyPartOp) keyPart).getKeys();
        ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);

        Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis = new Long2ObjectOpenHashMap<>(nodeIds.length);

        for (int i = 0; i < nodeIds.length; i += 1) {
            NearestNeighborsIElement tv = row == null ? null
                : (NearestNeighborsIElement)(row.get(nodeIds[i]));
            // Queries with no neighbour found have no result entry. Return an empty
            // neighbour list instead of failing the whole save job with an NPE.
            resKeysDis.put(nodeIds[i], tv == null ? new NeighborDistance[0] : tv.getFoundKeysDis());
        }

        return new GetSearchResultPartResult(resKeysDis);
    }

    @Override
    public GetResult merge(List<PartitionGetResult> partResults) {
        int size = 0;
        for (PartitionGetResult result: partResults) {
            size += ((GetSearchResultPartResult) result).size();
        }
        Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis = new Long2ObjectOpenHashMap<>(size);

        for (PartitionGetResult result: partResults) {
            GetSearchResultPartResult r = (GetSearchResultPartResult) result;
            resKeysDis.putAll(r.getResKeysDis());
        }
        return new GetSearchResultResult(resKeysDis);

    }

}
