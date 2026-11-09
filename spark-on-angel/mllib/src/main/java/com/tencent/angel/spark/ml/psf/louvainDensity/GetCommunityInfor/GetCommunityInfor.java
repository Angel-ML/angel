package com.tencent.angel.spark.ml.psf.louvainDensity.GetCommunityInfor;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.spark.ml.psf.louvainDensity.InitCommunityInfor.CommunityInforElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import java.util.List;

/**
 * get community's information data
 */
public class GetCommunityInfor extends GetFunc {

  public GetCommunityInfor(GetParam param) {
    super(param);
  }

  public GetCommunityInfor() {
    this(null);
  }

  /**
   * get Result according to ps matrix's partition
   *
   * @param partParam the partition parameter
   */
  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    PartGetCommunityInforParam param = (PartGetCommunityInforParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());
    ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(0));
    long[] nodeIds = param.getNodeIds();

    CommunityInforElement[] elements = new CommunityInforElement[nodeIds.length];

    for (int i = 0; i < nodeIds.length; i++) {
      //getNode neighbors
      CommunityInforElement elem = (CommunityInforElement) (row.get(nodeIds[i]));

      if (elem == null) {
        elements[i] = new CommunityInforElement();
      } else {
        elements[i] = elem;
      }
    }

    return new PartGetCommunityInforResult(part.getPartitionKey().getPartitionId(), elements);
  }

  /**
   * merge result
   *
   * @param partResults the partition results
   */
  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(
        partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartGetCommunityInforResult) result).getPartId(), result);
    }

    GetCommunityInforParam param = (GetCommunityInforParam) getParam();

    long[] nodeIs = param.getNodeIds();
    List<PartitionGetParam> partParams = param.getPartParams();

    // the NeighborsAttrTagElement result to return
    Long2ObjectOpenHashMap<CommunityInforElement> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(
        nodeIs.length);

    for (PartitionGetParam partParam : partParams) {
      int start = ((PartGetCommunityInforParam) partParam).getStartIndex();
      int end = ((PartGetCommunityInforParam) partParam).getEndIndex();

      PartGetCommunityInforResult partResult = (PartGetCommunityInforResult) (partIdToResultMap
          .get(partParam.getPartKey().getPartitionId()));

      CommunityInforElement[] results = partResult.getCommunitiesInfor();
      for (int i = start; i < end; i++) {
        nodeIdToNeighbors.put(nodeIs[i], results[i - start]);
      }
    }
    return new GetCommunityInforResult(nodeIdToNeighbors);
  }

}
