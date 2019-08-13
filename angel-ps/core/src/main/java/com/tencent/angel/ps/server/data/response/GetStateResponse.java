package com.tencent.angel.ps.server.data.response;

public class GetStateResponse extends Response {

  public GetStateResponse(ResponseType responseType, String detail) {
    super(responseType, detail);
  }

  public GetStateResponse(ResponseType responseType) {
    this(responseType, "");
  }

  public GetStateResponse() {
    this(ResponseType.SUCCESS, "");
  }
}
