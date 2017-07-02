/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.spark.graphx.util

/**
 * Wrapper around an iterator which calls a completion method after it successfully iterates
 * through all the elements.
 */
// scalastyle:off
private[graphx] abstract class CompletionIterator[+A, +I <: Iterator[A]](sub: I)
  extends Iterator[A] {
  // scalastyle:on

  private[this] var completed = false

  def next(): A = sub.next()

  def hasNext: Boolean = {
    val r = sub.hasNext
    if (!r && !completed) {
      completed = true
      completion()
    }
    r
  }

  def completion(): Unit
}

private[graphx] object CompletionIterator {
  def apply[A, I <: Iterator[A]](sub: I, completionFunction: => Unit): CompletionIterator[A, I] = {
    new CompletionIterator[A, I](sub) {
      def completion(): Unit = completionFunction
    }
  }
}
