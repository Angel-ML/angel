package com.tencent.angel.ml.matrix.psf.get.base;

import java.util.ArrayList;
import java.util.List;

import com.tencent.angel.PartitionKey;
import com.tencent.angel.psagent.PSAgentContext;

/**
 * The Get parameter. The class that extends this interface must have a constructor without
 * parameters.
 */
public class GetParam implements ParamSplit {
  /**
   * The Matrix id.
   */
  public final int matrixId;

  /**
   * Creates a new Get parameter.
   *
   * @param matrixId the matrix id
   */
  public GetParam(int matrixId) {
    this.matrixId = matrixId;
  }

  public GetParam() {
    this(-1);
  }

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public int getMatrixId() {
    return matrixId;
  }

  @Override
  public List<PartitionGetParam> split() {
    List<PartitionKey> parts =
        PSAgentContext.get().getMatrixPartitionRouter().getPartitionKeyList(matrixId);
    int size = parts.size();

    List<PartitionGetParam> partParams = new ArrayList<PartitionGetParam>(size);

    for (int i = 0; i < size; i++) {
      partParams.add(new PartitionGetParam(matrixId, parts.get(i)));
    }

    return partParams;
  }
}
