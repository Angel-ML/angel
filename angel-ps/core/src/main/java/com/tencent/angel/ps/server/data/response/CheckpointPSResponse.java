package com.tencent.angel.ps.server.data.response;

public class CheckpointPSResponse extends Response {
  public CheckpointPSResponse(ResponseType responseType, String detail) {
    super(responseType, detail);
  }

  public CheckpointPSResponse() {
    this(ResponseType.SUCCESS, "");
  }
}
