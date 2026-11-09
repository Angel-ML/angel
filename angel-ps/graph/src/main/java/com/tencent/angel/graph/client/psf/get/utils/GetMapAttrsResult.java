package com.tencent.angel.graph.client.psf.get.utils;

import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.Map;

public class GetMapAttrsResult extends GetResult {
  private final Long2ObjectOpenHashMap<Map<Integer, float[]>> nodeIdToContents;

  public GetMapAttrsResult(Long2ObjectOpenHashMap<Map<Integer, float[]>> nodeIdToContents) {
    this.nodeIdToContents = nodeIdToContents;
  }

  public Long2ObjectOpenHashMap<Map<Integer, float[]>> getNodeIdToContents() {
    return nodeIdToContents;
  }
}
