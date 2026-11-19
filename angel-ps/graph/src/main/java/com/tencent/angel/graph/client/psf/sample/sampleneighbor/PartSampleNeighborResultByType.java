package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class PartSampleNeighborResultByType extends PartSampleNeighborResult {

  public PartSampleNeighborResultByType(int partId,
      Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors, NeighborStorageType storageType) {
    super(partId, nodeIdToSampleNeighbors, storageType);
  }

  public PartSampleNeighborResultByType() {
    this(-1, null, NeighborStorageType.LONGARRAY);
  }

  public int getPartId() {
    return partId;
  }
}
