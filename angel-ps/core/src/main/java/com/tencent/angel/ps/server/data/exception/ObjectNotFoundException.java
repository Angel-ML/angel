package com.tencent.angel.ps.server.data.exception;

public class ObjectNotFoundException extends RuntimeException {
  public ObjectNotFoundException(String log) {
    super(log);
  }
}
