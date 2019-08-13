package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.ParameterServerId;
import com.tencent.angel.ps.server.data.TransportMethod;
import io.netty.buffer.ByteBuf;

public class CheckpointPSRequest extends PSRequest {

  private int matrixId;
  private int checkPointId;

  public CheckpointPSRequest(int userRequestId, int matrixId, int checkPointId, ParameterServerId psId) {
    super(userRequestId, psId);
    this.matrixId = matrixId;
    this.checkPointId = checkPointId;
  }

  public CheckpointPSRequest() {
    this(-1, -1, -1, null);
  }

  public int getMatrixId() {
    return matrixId;
  }

  public void setMatrixId(int matrixId) {
    this.matrixId = matrixId;
  }

  public int getCheckPointId() {
    return checkPointId;
  }

  public void setCheckPointId(int checkPointId) {
    this.checkPointId = checkPointId;
  }

  @Override
  public int getEstimizeDataSize() {
    return 0;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(matrixId);
    buf.writeInt(checkPointId);
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    matrixId = buf.readInt();
    checkPointId = buf.readInt();
  }

  @Override public int bufferLen() {
    return 4 + 4 + super.bufferLen();
  }

  @Override
  public TransportMethod getType() {
    return TransportMethod.CHECKPOINT;
  }

  @Override
  public boolean timeoutEnable() {
    return false;
  }
}
