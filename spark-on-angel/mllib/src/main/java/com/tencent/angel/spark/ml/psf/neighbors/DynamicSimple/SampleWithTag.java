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

public class SampleWithTag extends GetFunc {

  public SampleWithTag(SampleWithTagParam param) {
    super(param);
  }

  public SampleWithTag() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    return partitionSampleWithTag(psContext, partParam);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(partResults.size());
    int resultSize = 0;
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartSampleWithTagResult) result).getPartId(), result);
      resultSize += ((PartSampleWithTagResult) result).getNodeIds().length;
    }

    long[] resultNodes = new long[resultSize];
    int[] resultTags = new int[resultSize];
    int startIndex = 0;

    SampleWithTagParam param = (SampleWithTagParam) getParam();
    List<PartitionGetParam> partParams = param.getPartParams();
    for (PartitionGetParam partParam: partParams) {
      PartSampleWithTagResult partResult = (PartSampleWithTagResult) (partIdToResultMap.get(partParam.getPartKey().getPartitionId()));
      long[] nodeIds = partResult.getNodeIds();
      int[] tags = ((PartSampleWithTagParam) partParam).getTags();
      System.arraycopy(nodeIds, 0, resultNodes, startIndex, nodeIds.length);
      System.arraycopy(tags, 0, resultTags, startIndex, tags.length);
      startIndex += nodeIds.length;
    }
    return new SampleWithTagResult(resultNodes, resultTags);
  }

  public static PartitionGetResult partitionSampleWithTag(PSContext psContext,
                                                          PartitionGetParam partParam) {
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
    return new PartSampleWithTagResult(param.getPartKey().getPartitionId(), result);
  }


}
