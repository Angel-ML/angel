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
package com.tencent.angel.spark.ml.embedding.line

import java.io.{DataInputStream, DataOutputStream}

import com.tencent.angel.model.output.format.{ComplexRowFormat, IndexAndElement}
import com.tencent.angel.ps.storage.vector.element.IElement
import org.apache.hadoop.conf.Configuration

/**
  * The output format for LINE
  * @param conf
  */
class TextLINEModelOutputFormat(conf:Configuration) extends ComplexRowFormat(conf) {
  val featSep: String = conf.get("line.feature.sep", " ")
  val keyValueSep: String = conf.get("line.keyvalue.sep", ":")
  val modelOrder: Int = conf.get("line.model.order", "2").toInt

  override def load(input: DataInputStream): IndexAndElement = {
    val line = input.readLine()
    val indexAndElement = new IndexAndElement
    val keyValues = line.split(keyValueSep)

    if(featSep.equals(keyValueSep)) {
      indexAndElement.index = keyValues(0).toLong
      val feats = new Array[Float](keyValues.length - 1)
      (1 until keyValues.length).foreach(i => feats(i - 1) = keyValues(i).toFloat)
      indexAndElement.element = new LINENode(feats, null)
    } else {
      indexAndElement.index = keyValues(0).toLong
      val inputFeats = keyValues(1).split(featSep).map(f => f.toFloat)
      if(modelOrder == 1) {
        indexAndElement.element = new LINENode(inputFeats, null)
      } else {
        indexAndElement.element = new LINENode(inputFeats, new Array[Float](inputFeats.length))
      }

    }
    indexAndElement
  }

  override def save(key: Long, value: IElement, output: DataOutputStream): Unit = {
    val sb = new StringBuilder

    // Write node id
    sb.append(key)
    sb.append(keyValueSep)

    // Write feats
    val feats = value.asInstanceOf[LINENode].getInputFeats

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
}
