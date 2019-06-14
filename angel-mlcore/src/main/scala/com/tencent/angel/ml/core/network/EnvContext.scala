package com.tencent.angel.ml.core.network

trait EnvContext[T] {
  def client: T
}