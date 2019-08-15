package com.tencent.angel.ps.server.data.request;

import com.tencent.angel.ps.ParameterServerId;
import io.netty.buffer.ByteBuf;

public abstract class PSRequest extends Request {
  private ParameterServerId psId;
  /**
   * Create a new Request.
   *
   * @param userRequestId user request id
   */
  public PSRequest(int userRequestId, ParameterServerId psId) {
    super(userRequestId, new RequestContext());
    this.psId = psId;
  }

  public ParameterServerId getPsId() {
    return psId;
  }

  @Override public void serialize(ByteBuf buf) {
    super.serialize(buf);
    buf.writeInt(psId.getIndex());
  }

  @Override public void deserialize(ByteBuf buf) {
    super.deserialize(buf);
    psId = new ParameterServerId(buf.readInt());
  }

  @Override public int bufferLen() {
    return 4 + super.bufferLen();
  }
}
