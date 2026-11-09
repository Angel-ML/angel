package com.tencent.angel.graph.client.initedgefeatures;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.exception.AngelException;
import com.tencent.angel.graph.utils.LongIndexComparator;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateParam;
import com.tencent.angel.psagent.PSAgentContext;
import com.tencent.angel.psagent.matrix.oplog.cache.RowUpdateSplitUtils;
import it.unimi.dsi.fastutil.ints.IntArrays;
import java.util.ArrayList;
import java.util.List;

public class InitEdgeFeatsParam extends UpdateParam {

  private final long[] keys;
  private final int[] indptr;
  private final long[] neighbors;
  private int[] types;
  private final int edgeDim;
  private final IntFloatVector[] feats;
  private final int startIndex;
  private final int endIndex;

  public InitEdgeFeatsParam(int matrixId, long[] keys, int[] indptr, long[] neighbors, int[] types,
      int edgeDim, IntFloatVector[] feats, int start, int end) {
    super(matrixId);
    this.keys = keys;
    this.indptr = indptr;
    this.neighbors = neighbors;
    this.types = types;
    this.edgeDim = edgeDim;
    this.feats = feats;
    this.startIndex = start;
    this.endIndex = end;
    assert (keys.length == feats.length);
  }

  public InitEdgeFeatsParam(int matrixId, long[] keys, int[] indptr, long[] neighbors, int edgeDim,
      IntFloatVector[] feats) {
    this(matrixId, keys, indptr, neighbors, null, edgeDim, feats, 0, keys.length);
  }

  public InitEdgeFeatsParam(int matrixId, long[] keys, int[] indptr, long[] neighbors, int[] types,
      int edgeDim,
      IntFloatVector[] feats) {
    this(matrixId, keys, indptr, neighbors, null, edgeDim, feats, 0, keys.length);
  }


  @Override
  public List<PartitionUpdateParam> split() {
    LongIndexComparator comparator = new LongIndexComparator(keys);
    int size = endIndex - startIndex;
    int[] index = new int[size];
    for (int i = 0; i < size; i++) {
      index[i] = i + startIndex;
    }
    IntArrays.quickSort(index, comparator);

    List<PartitionUpdateParam> params = new ArrayList<>();
    List<PartitionKey> parts = PSAgentContext.get().getMatrixMetaManager().getPartitions(matrixId);

    if (!RowUpdateSplitUtils.isInRange(keys, index, parts)) {
      throw new AngelException(
          "node id is not in range [" + parts.get(0).getStartCol() + ", " + parts
              .get(parts.size() - 1).getEndCol());
    }

    int nodeIndex = startIndex;
    int partIndex = 0;
    while (nodeIndex < endIndex || partIndex < parts.size()) {
      int length = 0;
      long endOffset = parts.get(partIndex).getEndCol();
      while (nodeIndex < endIndex && keys[index[nodeIndex - startIndex]] < endOffset) {
        nodeIndex++;
        length++;
      }

      if (length > 0) {
        params.add(new InitEdgeFeatsPartParam(matrixId,
            parts.get(partIndex), keys, index, indptr, neighbors, types, edgeDim, feats,
            nodeIndex - length - startIndex, nodeIndex - startIndex));
      }

      partIndex++;
    }

    return params;
  }
}
