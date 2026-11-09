package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyIntValuePartOp;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import java.util.List;
import scala.Tuple2;

public class SampleNeighborWithTime extends SampleNeighbor {

  public SampleNeighborWithTime(SampleNeighborWithTimeParam param) {
    super(param);
  }

  public SampleNeighborWithTime() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborWithTimeParam sampleParam = (PartSampleNeighborWithTimeParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyIntValuePartOp split = (ILongKeyIntValuePartOp) sampleParam.getIndicesToTimePart();
    long[] nodeIds = split.getKeys();
    int[] timestamps = split.getValues();
    int count = sampleParam.getCount();
    NeighborStorageType storageType = sampleParam.getStorageType();
    SampleMethod sampleMethod = sampleParam.getMethod();

    Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeId2SampleNeighbors = SampleUtils
        .sampleWithTime(row, nodeIds, timestamps, sampleMethod, count, storageType,
            System.currentTimeMillis());

    return new PartSampleNeighborResultWithTime(sampleParam.getPartKey().getPartitionId(),
        nodeId2SampleNeighbors, storageType);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> nodeId2SampleNeighbors =
        new Object2ObjectOpenHashMap<>(((SampleNeighborWithTimeParam) param).getNodeIds().length);

    for (PartitionGetResult partResult : partResults) {
      // Sample part result
      PartSampleNeighborResultWithTime partSampleResult = (PartSampleNeighborResultWithTime) partResult;

      // Neighbors
      Object2ObjectOpenHashMap<Tuple2<Long, Integer>, Tuple2<Neighbor, int[]>> partNodeIdToSampleNeighbors = partSampleResult
          .getNodeIdToSampleNeighbors();
      if (partNodeIdToSampleNeighbors != null) {
        nodeId2SampleNeighbors.putAll(partNodeIdToSampleNeighbors);
      }
    }

    return new SampleNeighborResultWithTime(nodeId2SampleNeighbors);
  }
}
