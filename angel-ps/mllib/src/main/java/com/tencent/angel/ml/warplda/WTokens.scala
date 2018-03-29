/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.warplda

import scala.collection.mutable.ArrayBuffer

/**
  * Created by chris on 8/22/17.
  */
class Document() {
  var docId: Int = _
  var len: Int = _
  var wids: Array[Int] = _

  def this(docId: Int, len: Int, wids: Array[Int]) {
    this()
    this.docId = docId
    this.len = len
    this.wids = wids
  }

  def this(str: String) {
    this()
    val parts = str.split("\t")
    this.docId = Integer.parseInt(parts(0))
    this.wids = parts(1).split(" ").map(f => Integer.parseInt(f))
    this.len = wids.length
  }

  def this(docId: Int, wids: Array[Int]) {
    this()
    this.docId = docId
    this.wids = wids
    this.len = wids.length
  }

}


class WTokens(val n_words: Int, val n_docs: Int) {
  var ws: Array[Int] = _
  var topics: Array[Int] = _
  var docLens: Array[Int] = _
  var docIds: Array[Int] = _
  var mhProp: Array[Array[Int]] = _
  var n_tokens: Int = _
  var inverseMatrix: Array[Int] = _
  var accDoc: Array[Int] = _


  def build(docs: ArrayBuffer[Document], K: Int, mh: Int): Unit = {
    val wcnt = Array.ofDim[Int](n_words)
    this.ws = Array.ofDim[Int](n_words + 1)
    this.accDoc = Array.ofDim[Int](n_docs + 1)
    this.docLens = Array.ofDim[Int](n_docs)
    this.docIds = Array.ofDim[Int](n_docs)
    n_tokens = 0
    docs.indices foreach { d =>
      val doc = docs(d)
      n_tokens += doc.len
      docLens(d) = doc.len
      docIds(d) = doc.docId
      (0 until doc.len) foreach { w =>
        wcnt(doc.wids(w)) += 1
      }
    }
    this.topics = Array.ofDim[Int](n_tokens)
    this.inverseMatrix = Array.ofDim[Int](n_tokens)
    //word count
    ws(0) = 0
    (0 until n_words) foreach { i =>
      ws(i + 1) = ws(i) + wcnt(i)
    }
    this.topics = Array.ofDim[Int](n_tokens)
    this.mhProp = Array.ofDim[Int](mh, n_tokens)
    //doc count
    accDoc(0) = 0
    (0 until n_docs) foreach { i =>
      accDoc(i + 1) = accDoc(i) + docLens(i)
    }

    var start = 0
    (0 until n_docs) foreach { d =>
      val doc = docs(d)
      (0 until doc.len) foreach { w =>
        val wid = doc.wids(w)
        inverseMatrix(start) = ws(wid) + {
          wcnt(wid) -= 1
          wcnt(wid)
        }
        start += 1
      }
    }
  }
}
