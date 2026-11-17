package com.tencent.angel.spark.ml.psf.neighbors.DynamicSimple;

import com.tencent.angel.graph.model.neighbor.dynamicsimple.DynamicSimpleNeighborElement;
import com.tencent.angel.graph.utils.GraphMatrixUtils;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.PSContext;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;

import java.util.List;
import java.util.Random;

public class SampleWithTagByNodeId extends GetFunc {

  public SampleWithTagByNodeId(SampleWithTagParam param) {
    super(param);
  }

  public SampleWithTagByNodeId() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return partitionSampleWithTagByNodeId(psContext, partParam);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(partResults.size());
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartSampleWithTagByNodeIdResult) result).getPartId(), result);
      resultSize += ((PartSampleWithTagByNodeIdResult) result).getNodeIds().length;
    }

    long[] sourceNodes = new long[resultSize];
    long[] resultNodes = new long[resultSize];
    int[] resultTags = new int[resultSize];
    int startIndex = 0;

    SampleWithTagParam param = (SampleWithTagParam) getParam();
    List<PartitionGetParam> partParams = param.getPartParams();
    for (PartitionGetParam partParam: partParams) {
      PartSampleWithTagByNodeIdResult partResult = (PartSampleWithTagByNodeIdResult) (partIdToResultMap.get(partParam.getPartKey().getPartitionId()));
      long[] nodeIds = partResult.getNodeIds();
      long[] neighbors = partResult.getNeighbors();
      int[] tags = ((PartSampleWithTagParam) partParam).getTags();
      System.arraycopy(nodeIds, 0, sourceNodes, startIndex, nodeIds.length);
      System.arraycopy(neighbors, 0, resultNodes, startIndex, neighbors.length);
      System.arraycopy(tags, 0, resultTags, startIndex, tags.length);
      startIndex += nodeIds.length;
    }
    return new SampleWithTagByNodeIdResult(sourceNodes, resultNodes, resultTags);
  }

  public static PartitionGetResult partitionSampleWithTagByNodeId(PSContext psContext, PartitionGetParam partParam) {
    PartSampleWithTagParam param = (PartSampleWithTagParam) partParam;
    long[] nodeIds = param.getNodeIds();
    long[] result = new long[nodeIds.length];
    Random r = new Random();

    ServerLongAnyRow row = GraphMatrixUtils.getPSLongKeyRow(psContext, param);
    for (int i = 0; i < nodeIds.length; i++) {
      long nodeId = nodeIds[i];
      DynamicSimpleNeighborElement element = (DynamicSimpleNeighborElement) (row.get(nodeId));
      if (element == null) {
        result[i] = nodeId;
      } else {
        result[i] = element.sample(r, nodeId);
      }
    }
    return new PartSampleWithTagByNodeIdResult(param.getPartKey().getPartitionId(), nodeIds, result);
  }


}
