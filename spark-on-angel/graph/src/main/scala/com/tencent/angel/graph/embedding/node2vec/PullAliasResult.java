package com.tencent.angel.graph.embedding.node2vec;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.psagent.matrix.ResponseType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import scala.Tuple3;

public class PullAliasResult extends GetResult {
    private Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> result;

    public PullAliasResult(Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> result) {
        super();
        this.result = result;
    }

    public PullAliasResult(ResponseType responseType, Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> result) {
        super(responseType);
        this.result = result;
    }

    public PullAliasResult(ResponseType responseType) {
        super(responseType);
    }

    public PullAliasResult() {
        super();
    }

    public Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> getResult() {
        return result;
    }

    public void setResult(Long2ObjectOpenHashMap<Tuple3<long[], float[], int[]>> result) {
        this.result = result;
    }
}