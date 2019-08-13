package com.tencent.angel.psagent.matrix.transport.adapter;

public class CheckpointRequest extends UserRequest {
  private final int matrixId;
  /**
   * Create a new UserRequest
   *
   * @param type request type
   */
  public CheckpointRequest(UserRequestType type, int matrixId) {
    super(type);
    this.matrixId = matrixId;
  }

  public int getMatrixId() {
    return matrixId;
  }
}
