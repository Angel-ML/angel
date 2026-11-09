package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

public class SampleNeighbor extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNeighbor(SampleNeighborParam param) {
    super(param);
  }

  public SampleNeighbor() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborParam sampleParam = (PartSampleNeighborParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyPartOp split = (ILongKeyPartOp) sampleParam.getIndicesPart();
    long[] nodeIds = split.getKeys();

    Long2ObjectOpenHashMap<Neighbor> nodeId2SampleNeighbors = null;
    SampleMethod method = sampleParam.getMethod();
    NeighborStorageType storageType = sampleParam.getStorageType();
    switch (method) {
      case RANDOM:
        nodeId2SampleNeighbors = SampleUtils.sampleByCount(row, sampleParam.getCount(),
            nodeIds, storageType, System.currentTimeMillis());
        break;
      case ALIASTABLE:
      case SIMPLEREJECT:
        nodeId2SampleNeighbors = SampleUtils.sampleByWeight(row, sampleParam.getCount(),
            nodeIds, method, storageType, System.currentTimeMillis());
        break;

    }

    return new PartSampleNeighborResult(sampleParam.getPartKey().getPartitionId(),
        nodeId2SampleNeighbors, storageType);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors =
        new Long2ObjectOpenHashMap<>(((SampleNeighborParam) param).getNodeIds().length);

    for (PartitionGetResult partResult : partResults) {
      // Sample part result
      PartSampleNeighborResult partSampleResult = (PartSampleNeighborResult) partResult;

      // Neighbors
      Long2ObjectOpenHashMap<Neighbor> partNodeIdToSampleNeighbors = partSampleResult
          .getNodeIdToSampleNeighbors();
      if (partNodeIdToSampleNeighbors != null) {
        nodeIdToSampleNeighbors.putAll(partNodeIdToSampleNeighbors);
      }
    }

    return new SampleNeighborResult(nodeIdToSampleNeighbors);
  }
}
