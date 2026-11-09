package com.tencent.angel.graph.client.psf.sample.sampleedgefeats;

import com.tencent.angel.graph.client.psf.sample.SampleUtils;
import com.tencent.angel.graph.data.Feature;
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

public class SampleEdgeFeat extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public SampleEdgeFeat(SampleEdgeFeatParam param) {
    super(param);
  }

  public SampleEdgeFeat() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartSampleEdgeFeatParam sampleParam = (PartSampleEdgeFeatParam) partParam;
    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, partParam);

    ILongKeyPartOp split = (ILongKeyPartOp) sampleParam.getIndicesPart();
    long[] nodeIds = split.getKeys();

    Tuple2<Long2ObjectOpenHashMap<Neighbor>, Long2ObjectOpenHashMap<Feature[]>> nodeId2SampleNeighbors =
        SampleUtils
            .sampleEdgeFeat(row, sampleParam.getCount(), nodeIds, sampleParam.getMethod(),
                sampleParam.getStorageType(), System.currentTimeMillis());

    return new PartSampleEdgeFeatResult(sampleParam.getPartKey().getPartitionId(),
        nodeId2SampleNeighbors._1, nodeId2SampleNeighbors._2, sampleParam.getStorageType());
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Long2ObjectOpenHashMap<Neighbor> nodeIdToSampleNeighbors =
        new Long2ObjectOpenHashMap<>(((SampleEdgeFeatParam) param).getNodeIds().length);
    Long2ObjectOpenHashMap<Feature[]> nodeIdToEdgeFeats =
        new Long2ObjectOpenHashMap<>(((SampleEdgeFeatParam) param).getNodeIds().length);

    for (PartitionGetResult partResult : partResults) {
      // Sample part result
      PartSampleEdgeFeatResult partSampleResult = (PartSampleEdgeFeatResult) partResult;

      // Neighbors
      Long2ObjectOpenHashMap<Neighbor> partNodeIdToSampleNeighbors = partSampleResult
          .getNodeIdToSampleNeighbors();
      if (partNodeIdToSampleNeighbors != null) {
        nodeIdToSampleNeighbors.putAll(partNodeIdToSampleNeighbors);
      }

      // EdgeFeatures
      if (partNodeIdToSampleNeighbors != null) {
        nodeIdToEdgeFeats.putAll(partSampleResult
            .getNodeIdToSampleEdgeFeats());
      }
    }
    return new SampleEdgeFeatResult(nodeIdToSampleNeighbors, nodeIdToEdgeFeats);
  }
}
