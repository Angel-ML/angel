package com.tencent.angel.graph.kclique;

import com.tencent.angel.ml.matrix.psf.get.base.GetFunc;
import com.tencent.angel.ml.matrix.psf.get.base.GetResult;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetResult;
import com.tencent.angel.ps.storage.matrix.ServerMatrix;
import com.tencent.angel.ps.storage.partition.RowBasedPartition;
import com.tencent.angel.ps.storage.partition.ServerPartition;
import com.tencent.angel.ps.storage.vector.ServerLongAnyRow;
import com.tencent.angel.ps.storage.vector.element.LongArrayElement;
import it.unimi.dsi.fastutil.ints.Int2ObjectArrayMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;

import java.util.List;
import java.util.Random;

/**
 * Sample the neighbor
 */
public class SampleNodeInfo extends GetFunc {

  public SampleNodeInfo(KCliqueGetParam param) {
    super(param);
  }

  public SampleNodeInfo() {
    this(null);
  }

  @Override
  public PartitionGetResult partitionGet(PartitionGetParam partParam) {
    KCliquePartGetParam param = (KCliquePartGetParam) partParam;
    ServerMatrix matrix = psContext.getMatrixStorageManager().getMatrix(partParam.getMatrixId());
    ServerPartition part = matrix.getPartition(partParam.getPartKey().getPartitionId());

    int readRowId = param.getReadRowId();
    ServerLongAnyRow row = (ServerLongAnyRow) (((RowBasedPartition) part).getRow(readRowId));
    long[] nodeIds = param.getNodeIds();
    long[][] neighbors = new long[nodeIds.length][];

    int count = param.getCount();
    Random r = new Random();

    for (int i = 0; i < nodeIds.length; i++) {
      long nodeId = nodeIds[i];

      // Get node neighbor number
      LongArrayElement element = (LongArrayElement) (row.get(nodeId));
      if (element == null) {
        neighbors[i] = null;
      } else {
        long[] nodeNeighbors = element.getData();
        if (nodeNeighbors == null || nodeNeighbors.length == 0) {
          neighbors[i] = null;
        } else if (count <= 0 || nodeNeighbors.length <= count) {
          neighbors[i] = nodeNeighbors;
        } else {
          neighbors[i] = new long[count];
          // If the neighbor number > count, just copy a range of neighbors to the result array, the copy position is random
          int startPos = Math.abs(r.nextInt()) % nodeNeighbors.length;
          if (startPos + count <= nodeNeighbors.length) {
            System.arraycopy(nodeNeighbors, startPos, neighbors[i], 0, count);
          } else {
            System.arraycopy(nodeNeighbors, startPos, neighbors[i], 0, nodeNeighbors.length - startPos);
            System.arraycopy(nodeNeighbors, 0, neighbors[i], nodeNeighbors.length - startPos, count - (nodeNeighbors.length - startPos));
          }
        }
      }
    }

    return new PartSampleNodeInfoResult(part.getPartitionKey().getPartitionId(), neighbors);
  }

  @Override
  public GetResult merge(List<PartitionGetResult> partResults) {
    Int2ObjectArrayMap<PartitionGetResult> partIdToResultMap = new Int2ObjectArrayMap<>(partResults.size());
    for (PartitionGetResult result : partResults) {
      partIdToResultMap.put(((PartSampleNodeInfoResult) result).getPartId(), result);
    }

    KCliqueGetParam param = (KCliqueGetParam) getParam();
    long[] nodeIds = param.getNodeIds();
    List<PartitionGetParam> partParams = param.getPartParams();

    Long2ObjectOpenHashMap<long[]> nodeIdToNeighbors = new Long2ObjectOpenHashMap<>(nodeIds.length);

    for (PartitionGetParam partParam : partParams) {
      int start = ((KCliquePartGetParam) partParam).getStartIndex();
      int end = ((KCliquePartGetParam) partParam).getEndIndex();
      PartSampleNodeInfoResult partResult = (PartSampleNodeInfoResult) (partIdToResultMap.get(partParam.getPartKey().getPartitionId()));
      long[][] results = partResult.getNodeIdToNeighbors();
      for (int i = start; i < end; i++) {
        nodeIdToNeighbors.put(nodeIds[i], results[i - start]);
      }
    }

    return new SampleNodeInfoResult(nodeIdToNeighbors);
  }
}
