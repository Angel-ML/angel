package com.tencent.angel.graph.client.psf.sample.sampleneighbor;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.psagent.matrix.transport.router.operator.ILongKeyPartOp;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;

public class SampleNeighborByType extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleNeighborByType(SampleNeighborByTypeParam param) {
    super(param);
  }

  public SampleNeighborByType() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleNeighborByTypeParam sampleParam = (PartSampleNeighborByTypeParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyPartOp split = (ILongKeyPartOp) sampleParam.getIndicesPart();
    long[] nodeIds = split.getKeys();
    SampleType sampleType = sampleParam.getSampleType();
    switch (sampleType) {
      case NODE:
      case EDGE: {
        Long2ObjectOpenHashMap<long[]> nodeId2SampleNeighbors = SampleUtils
            .sampleByType(row, sampleParam.getCount(),
                nodeIds, sampleType, sampleParam.getNode_or_edge_type(), System.currentTimeMillis());
        return new PartSampleNeighborResultByType(sampleParam.getPartKey().getPartitionId(),
            nodeId2SampleNeighbors);
      }

      default: {
        throw new UnsupportedOperationException("Not support sample type " + sampleType + " now");
      }
    }
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<long[]> nodeIdToSampleNeighbors =
        new Long2ObjectOpenHashMap<>(((SampleNeighborParam) param).getNodeIds().length);

    for (PartitionGetResult partResult : partResults) {
      // Sample part result
      PartSampleNeighborResultByType partSampleResult = (PartSampleNeighborResultByType) partResult;

      // Neighbors
      Long2ObjectOpenHashMap<long[]> partNodeIdToSampleNeighbors = partSampleResult
          .getNodeIdToSampleNeighbors();
      if (partNodeIdToSampleNeighbors != null) {
        nodeIdToSampleNeighbors.putAll(partNodeIdToSampleNeighbors);
      }
    }

    return new SampleNeighborResultByType(nodeIdToSampleNeighbors);
  }
}
