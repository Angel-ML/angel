package com.tencent.angel.ml.matrix.psf.get.multi;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * The parameter of get row function.
 */
public class GetRowsParam extends GetParam {
  /** row indexes */
  private final List<Integer> rowIndexes;

  /**
   * Create a new GetRowsParam.
   *
   * @param matrixId matrix id
   * @param rowIndexes row indexes
   */
  public GetRowsParam(int matrixId, List<Integer> rowIndexes) {
    super(matrixId);
    this.rowIndexes = rowIndexes;
  }

  /**
   * Get row indexes.
   *
   * @return List<Integer> row indexes
   */
  public List<Integer> getRowIndexes() {
    return rowIndexes;
  }

  @Override
  public List<PartitionGetParam> split() {
    Map<PartitionKey, List<Integer>> parts =
        PSAgentContext.get().getMatrixPartitionRouter()
            .getPartitionKeyRowIndexesMap(matrixId, rowIndexes);

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(parts.size());

    for (Map.Entry<PartitionKey, List<Integer>> entry : parts.entrySet()) {
      partParams.add(new PartitionGetRowsParam(matrixId, entry.getKey(), entry.getValue()));
    }
    return partParams;
  }
}
