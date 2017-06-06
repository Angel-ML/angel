package com.tencent.angel.ml.matrix.psf.get.single;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.ml.matrix.psf.get.base.GetParam;
import com.tencent.angel.ml.matrix.psf.get.base.PartitionGetParam;
import com.tencent.angel.psagent.PSAgentContext;

import java.util.ArrayList;
import java.util.List;

/**
 * The parameter of get row function.
 */
public class GetRowParam extends GetParam {

  /** row index */
  private final int rowIndex;

  /**
   * Create a new GetRowParam.
   *
   * @param matrixId matrix id
   * @param rowIndex row index
   */
  public GetRowParam(int matrixId, int rowIndex) {
    super(matrixId);
    this.rowIndex = rowIndex;
  }

  /**
   * Get row index.
   *
   * @return int row index.
   */
  public int getRowIndex() {
    return rowIndex;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId, rowIndex);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (int i = 0; i < size; i++) {
      partParams.add(new PartitionGetRowParam(matrixId, rowIndex, parts.get(i)));
    }

    return partParams;
  }
}
