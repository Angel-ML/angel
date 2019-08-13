package com.tencent.angel.graph.client.getnodefeats2;

import com.tencent.angel.graph.data.Node;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.Collections;
import java.util.List;

public class GetNodeFeats extends GetFunc {

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public GetNodeFeats(GetNodeFeatsParam param) {
    super(param);
  }

  public GetNodeFeats() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetNodeFeatsParam param = (PartGetNodeFeatsParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    long[] nodeIds = param.getNodeIds();

    IntFloatVector[] feats = new IntFloatVector[nodeIds.length];
    for (int i = 0; i < nodeIds.length; i++) {
      if (row.get(nodeIds[i]) == null) {
        continue;
      }
      feats[i] = ((Node) (row.get(nodeIds[i]))).getFeats();
    }
    return new PartGetNodeFeatsResult(part.getPartitionKey().getPartitionId(), feats);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
      partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartGetNodeFeatsResult) result).getPartId(), result);
    }


    GetNodeFeatsParam param = (GetNodeFeatsParam) getParam();
    long[] nodeIds = param.getNodeIds();
    List<PartitionGetParam> partParams = param.getPartParams();

    Long2ObjectOpenHashMap<IntFloatVector> results = new Long2ObjectOpenHashMap<>(nodeIds.length);


    int size = partResults.size();
    for (int i = 0; i < size; i++) {
      PartGetNodeFeatsParam partParam = (PartGetNodeFeatsParam) partParams.get(i);
      PartGetNodeFeatsResult partResult = (PartGetNodeFeatsResult) partIdToResultMap
        .get(partParam.getPartKey().getPartitionId());

      int start = partParam.getStartIndex();
      int end = partParam.getEndIndex();
      IntFloatVector[] feats = partResult.getFeats();
      for (int j = start; j < end; j++) {
        if (feats[j - start] != null) {
          results.put(nodeIds[j], feats[j - start]);
        }
      }
    }
    return new GetNodeFeatsResult(results);
  }
}
