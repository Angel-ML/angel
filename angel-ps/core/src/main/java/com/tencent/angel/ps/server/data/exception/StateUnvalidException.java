package com.tencent.angel.ps.server.data.exception;

public class StateUnvalidException extends RuntimeException {
  public StateUnvalidException(String log) {
    super(log);
  }
}
