package com.tencent.angel.spark.ml.ann_core.psf.getIVFHnswVectors;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple2;

public class GetIVFHnswVectorsResult extends GetResult {
    private Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> vector;
    public GetIVFHnswVectorsResult(Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> vector) {
        this.vector = vector;
    }

    public Long2ObjectOpenHashMap<Tuple2<long[], float[][]>> getVector() {
        return vector;
    }
}
