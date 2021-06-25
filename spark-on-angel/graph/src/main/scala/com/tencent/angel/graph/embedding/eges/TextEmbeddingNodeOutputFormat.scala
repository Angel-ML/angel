/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */
package com.tencent.angel.graph.embedding.eges

import java.io.{DataInputStream, DataOutputStream}

import com.tencent.angel.model.output.format.{ComplexRowFormat, IndexAndElement}
import com.tencent.angel.ps.storage.vector.element.IElement
import org.apache.hadoop.conf.Configuration

/**
 * The output format for EmbeddingNode Model
 * @param conf
 */
class TextEmbeddingNodeOutputFormat(conf:Configuration) extends ComplexRowFormat(conf) {
  val featSep = conf.get("embedding.feature.sep", " ")
  val keyValueSep = conf.get("embedding.keyvalue.sep", ":")

  override def load(input: DataInputStream): IndexAndElement = {
    val embedding = input.readLine()
    val indexAndElement = new IndexAndElement
    val keyValues = embedding.split(keyValueSep)

    if(featSep.equals(keyValueSep)) {
      indexAndElement.index = keyValues(0).toLong
      val feats = new Array[Float](keyValues.length - 1)
      (1 until keyValues.length).foreach(i => feats(i - 1) = keyValues(i).toFloat)
      indexAndElement.element = new EmbeddingNode(feats)
    } else {
      indexAndElement.index = keyValues(0).toLong
      val feats = keyValues(1).split(featSep).map(f => f.toFloat)
      indexAndElement.element = new EmbeddingNode(feats)
    }
    indexAndElement
  }

  override def save(key: Long, value: IElement, output: DataOutputStream): Unit = {
    val sb = new StringBuilder

    // Write node id
    sb.append(key)
    sb.append(keyValueSep)

    // Write feats
    val feats = value.asInstanceOf[EmbeddingNode].getFeats

    var index = 0
    val len = feats.length
    feats.foreach(f => {
      if(index < len - 1) {
        sb.append(f).append(featSep)
      } else {
        sb.append(f).append("\n")
      }
      index += 1
    })

    output.writeBytes(sb.toString())
  }

  override def save(key: String, value: IElement, output: DataOutputStream): Unit = {}

  override def save(key: IElement, value: IElement, output: DataOutputStream): Unit = {}
}
