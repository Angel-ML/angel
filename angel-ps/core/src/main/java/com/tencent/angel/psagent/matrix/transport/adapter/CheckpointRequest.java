package com.tencent.angel.psagent.matrix.transport.adapter;

public class CheckpointRequest extends UserRequest {
  private final int matrixId;
  private final int checkPointId;

  /**
   * Create a new UserRequest
   *
   * @param type request type
   */
  public CheckpointRequest(UserRequestType type, int matrixId, int checkPointId) {
    super(type);
    this.matrixId = matrixId;
    this.checkPointId = checkPointId;
  }

  public int getMatrixId() {
    return matrixId;
  }

  public int getCheckPointId() {
    return checkPointId;
  }
}
