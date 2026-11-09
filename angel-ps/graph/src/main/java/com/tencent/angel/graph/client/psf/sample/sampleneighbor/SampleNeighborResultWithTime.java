package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import scala.Tuple2;

public class SampleNeighborResultWithTime extends GetResult {

  private Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeIdToSampleNeighbor;

  SampleNeighborResultWithTime(
      Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeIdToSampleNeighbor) {
    this.nodeIdToSampleNeighbor = nodeIdToSampleNeighbor;
  }

  SampleNeighborResultWithTime() {
    this(null);
  }

  public Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> getNodeIdToSampleNeighbor() {
    return nodeIdToSampleNeighbor;
  }

  public void setNodeIdToSampleNeighbor(
      Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeIdToSampleNeighbor) {
    this.nodeIdToSampleNeighbor = nodeIdToSampleNeighbor;
  }
}
