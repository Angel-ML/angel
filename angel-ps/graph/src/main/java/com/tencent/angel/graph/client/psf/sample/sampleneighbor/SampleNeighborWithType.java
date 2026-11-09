package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.data.Neighbor;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;
import scala.Tuple2;
import scala.Tuple3;

public class SampleNeighborWithType extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNeighborWithType(SampleNeighborWithTypeParam param) {
    super(param);
  }

  public SampleNeighborWithType() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborWithTypeParam sampleParam = (PartSampleNeighborWithTypeParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyPartOp split = (ILongKeyPartOp) sampleParam.getIndicesPart();
    long[] nodeIds = split.getKeys();

    SampleType sampleType = sampleParam.getSampleType();
    switch (sampleType) {
      case NODE:
      case EDGE: {
        Tuple2<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<int[]>> nodeId2SampleNeighbors = SampleUtils
            .sampleWithType(row, sampleParam.getCount(),
                nodeIds, sampleType, sampleParam.getMethod(), sampleParam.getStorageType(),
                System.currentTimeMillis());

        return new PartSampleNeighborResultWithType(sampleParam.getPartKey().getPartitionId(),
            nodeId2SampleNeighbors._1, nodeId2SampleNeighbors._2, sampleType,
            sampleParam.getStorageType());
      }

      case NODE_AND_EDGE: {
        Tuple3<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<int[]>,
            Long2ObjectOpenHashMap<int[]>> nodeId2SampleNeighbors = SampleUtils
            .sampleWithBothType(row,
                sampleParam.getCount(), nodeIds, sampleParam.getMethod(),
                sampleParam.getStorageType(), System.currentTimeMillis());

        return new PartSampleNeighborResultWithType(sampleParam.getPartKey().getPartitionId(),
            nodeId2SampleNeighbors._1(), nodeId2SampleNeighbors._2(), nodeId2SampleNeighbors._3(),
            sampleParam.getStorageType());
      }

      default: {
        throw new UnsupportedOperationException("Not support sample type " + sampleType + " now");
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    SampleType sampleType = ((SampleNeighborWithTypeParam) param).getSampleType();

    Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors =
        new Long2ObjectOpenHashMap<>(((SampleNeighborParam) param).getNodeIds().length);
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
