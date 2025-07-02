package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

public class SampleNeighborByName extends SampleNeighbor {

  public SampleNeighborByName(SampleNeighborByNameParam param) {
    super(param);
  }

  public SampleNeighborByName() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborByNameParam sampleParam = (PartSampleNeighborByNameParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyPartOp split = (ILongKeyPartOp) sampleParam.getIndicesPart();
    long[] nodeIds = split.getKeys();
    String name = sampleParam.getName();

    Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors =
        SampleUtils.sampleByName(row, sampleParam.getCount(),
            nodeIds, name, System.currentTimeMillis());

    return new PartSampleNeighborResult(sampleParam.getPartKey().getPartitionId(),
        nodeId2SampleNeighbors);
  }
}