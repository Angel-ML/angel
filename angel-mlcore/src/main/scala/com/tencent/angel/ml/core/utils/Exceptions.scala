package com.tencent.angel.ml.core.utils

case class NotInitialException(message: String) extends RuntimeException(message)

case class GraphInvalidate(message: String) extends RuntimeException(message)

case class VariableInvalidate(message: String) extends RuntimeException(message)

case class ValueNotAllowed(message: String) extends RuntimeException(message)

case class MethodNotImplement(message: String) extends RuntimeException(message)

case class MLException(message: String) extends RuntimeException(message)
