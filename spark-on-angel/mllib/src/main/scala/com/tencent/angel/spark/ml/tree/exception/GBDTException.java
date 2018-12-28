package com.tencent.angel.spark.ml.tree.exception;

public class GBDTException extends RuntimeException {
    public GBDTException() {
        super();
    }

    public GBDTException(String msg) {
        super(msg);
    }

    public GBDTException(Throwable cause) {
        super(cause);
    }

    public GBDTException(String msg, Throwable cause) {
        super(msg, cause);
    }
}
