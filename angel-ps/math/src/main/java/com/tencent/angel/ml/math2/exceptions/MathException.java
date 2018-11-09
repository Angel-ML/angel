package com.tencent.angel.ml.math2.exceptions;

public class MathException extends RuntimeException {
    private static final long serialVersionUID = 1L;

    public MathException() {
    }

    public MathException(String message) {
        super(message);
    }

    public MathException(String message, Throwable cause) {
        super(message, cause);
    }

    public MathException(Throwable cause) {
        super(cause);
    }
}
