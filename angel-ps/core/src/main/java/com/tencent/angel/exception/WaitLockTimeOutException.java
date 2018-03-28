package com.tencent.angel.exception;

public class WaitLockTimeOutException extends RuntimeException {
  /**
   *
   */
  private static final long serialVersionUID = 1L;
  private final long timeOutValue;

  public WaitLockTimeOutException(String errorMsg, long timeOutValue) {
    super(errorMsg);
    this.timeOutValue = timeOutValue;
  }

  public long getTimeOutValue() {
    return timeOutValue;
  }

  @Override
  public String toString() {
    return "WaitLockTimeOutException [timeOutValue=" + timeOutValue + "]";
  }
}
