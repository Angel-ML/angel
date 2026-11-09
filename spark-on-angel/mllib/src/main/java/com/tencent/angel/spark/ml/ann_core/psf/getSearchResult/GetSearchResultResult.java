package com.tencent.angel.spark.ml.ann_core.psf.getSearchResult;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.spark.ml.ann_core.NeighborDistance;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class GetSearchResultResult extends GetResult {
    private Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis;

    public GetSearchResultResult(Long2ObjectOpenHashMap<NeighborDistance[]> resKeysDis) {
        this.resKeysDis = resKeysDis;
    }

    public Long2ObjectOpenHashMap<NeighborDistance[]> getResKeysDis() {
        return resKeysDis;
    }

}
