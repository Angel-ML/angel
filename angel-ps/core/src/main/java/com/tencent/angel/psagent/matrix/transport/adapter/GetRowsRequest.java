package com.tencent.angel.psagent.matrix.transport.adapter;

import java.util.Arrays;

public class GetRowsRequest extends UserRequest {
  /**
   * matrix id
   */
  private final int matrixId;

  /**
   * row index
   */
  private final int[] rowIds;

  public GetRowsRequest(int matrixId, int[] rowIds) {
    super(UserRequestType.GET_ROWS);
    this.matrixId = matrixId;
    this.rowIds = rowIds;
  }

  public int getMatrixId() {
    return matrixId;
  }

  public int[] getRowIds() {
    return rowIds;
  }

  @Override
  public String toString() {
    return "GetRowsRequest{" +
        "matrixId=" + matrixId +
        ", rowIds=" + Arrays.toString(rowIds) +
        ", type=" + type +
        '}';
  }
}
