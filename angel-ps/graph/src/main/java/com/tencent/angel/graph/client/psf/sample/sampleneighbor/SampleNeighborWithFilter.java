package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.data.NeighborStorageType;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyLongValuePartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import scala.Tuple2;

public class SampleNeighborWithFilter extends SampleNeighborWithType {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNeighborWithFilter(SampleNeighborWithFilterParam param) {
    super(param);
  }

  public SampleNeighborWithFilter() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborWithFilterParam sampleParam = (PartSampleNeighborWithFilterParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyLongValuePartOp split = (ILongKeyLongValuePartOp) sampleParam.getIndicesToFilterPart();
    long[] nodeIds = split.getKeys();
    long[] filterWithoutNeighKeys = split.getValues();
    long[] filterWithNeighKeys = sampleParam.getFilterWithNeighKeys();
    int count = sampleParam.getCount();
    NeighborStorageType storageType = sampleParam.getStorageType();

    SampleType sampleType = sampleParam.getSampleType();
    switch (sampleType) {
      case NODE:
      case EDGE: {
        Tuple2<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<int[]>> nodeId2SampleNeighbors = SampleUtils
            .sampleWithType(row, nodeIds, sampleType, count, filterWithoutNeighKeys,
                filterWithNeighKeys, storageType, System.currentTimeMillis());

        return new PartSampleNeighborResultWithType(sampleParam.getPartKey().getPartitionId(),
            nodeId2SampleNeighbors._1, nodeId2SampleNeighbors._2, sampleType, storageType);
      }

      default: {
        throw new UnsupportedOperationException("Not support sample type " + sampleType + " now");
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    SampleType sampleType = ((SampleNeighborWithTypeParam) param).getSampleType();

    Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors = new Long2ObjectOpenHashMap<>(
        ((SampleNeighborParam) param).getNodeIds().length);

    Long2ObjectOpenHashMap<int[]> nodeIdToSampleNeighborType =
        new Long2ObjectOpenHashMap<>(((SampleNeighborParam) param).getNodeIds().length);
    Long2ObjectOpenHashMap<int[]> nodeIdToSampleEdgeType =
        new Long2ObjectOpenHashMap<>(((SampleNeighborParam) param).getNodeIds().length);

    for (PartitionGetResult partResult : partResults) {
      // Sample part result
      PartSampleNeighborResultWithType partSampleResult = (PartSampleNeighborResultWithType) partResult;

      // Neighbors
      Long2ObjectOpenHashMap<Neighbor> partNodeIdToSampleNeighbors = partSampleResult
          .getNodeIdToSampleNeighbors();
      if (partNodeIdToSampleNeighbors != null) {
        nodeIdToSampleNeighbors.putAll(partNodeIdToSampleNeighbors);
      }

      // Neighbors type
      Long2ObjectOpenHashMap<int[]> partNodeIdToSampleNeighborsType = partSampleResult
          .getNodeIdToSampleNeighborsType();
      if (partNodeIdToSampleNeighborsType != null) {
        nodeIdToSampleNeighborType.putAll(partNodeIdToSampleNeighborsType);
      }

      // Edge type
      Long2ObjectOpenHashMap<int[]> partNodeIdToSampleEdgesType = partSampleResult
          .getNodeIdToSampleEdgeType();
      if (partNodeIdToSampleEdgesType != null) {
        nodeIdToSampleEdgeType.putAll(partNodeIdToSampleEdgesType);
      }
    }

    if (sampleType == SampleType.NODE) {
      return new SampleNeighborResultWithType(nodeIdToSampleNeighbors,
          nodeIdToSampleNeighborType, null);
    } else if (sampleType == SampleType.EDGE) {
      return new SampleNeighborResultWithType(nodeIdToSampleNeighbors, null,
          nodeIdToSampleEdgeType);
    } else {
      return new SampleNeighborResultWithType(nodeIdToSampleNeighbors, nodeIdToSampleNeighborType,
          nodeIdToSampleEdgeType);

    }
  }
}
