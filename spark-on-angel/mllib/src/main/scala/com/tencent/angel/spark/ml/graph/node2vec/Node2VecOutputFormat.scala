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

package com.tencent.angel.spark.ml.graph.node2vec

import java.io.{DataInputStream, DataOutputStream}

import com.tencent.angel.graph.client.node2vec.data.WalkPath
import com.tencent.angel.model.output.format.{ComplexRowFormat, IndexAndElement}
import com.tencent.angel.ps.storage.vector.element.IElement
import org.apache.hadoop.conf.Configuration

class Node2VecOutputFormat(conf: Configuration) extends ComplexRowFormat(conf) {
  val keyModSep: String = conf.get("node2vecnode2vec.keymod.sep", "#")
  val keyValueSep: String = conf.get("node2vecnode2vec.keyvalue.sep", ":")
  val featSep: String = conf.get("node2vec.feature.sep", " ")

  override def load(input: DataInputStream): IndexAndElement = {
    // val reader = new BufferedReader(new InputStreamReader(input))
    val line = input.readLine()
    val indexAndElement = new IndexAndElement
    val keyValues = line.split(keyValueSep)

    val key_mod = keyValues.head.split(keyModSep)
    indexAndElement.index = key_mod.head.toLong
    val mod = key_mod(1).toInt

    // println(line)
    if (featSep.equals(keyValueSep)) {
      indexAndElement.element = new WalkPath(keyValues.tail.map(_.toLong), mod)
    } else {
      indexAndElement.element = new WalkPath(keyValues(1).split(featSep).map(_.toLong), mod)
    }

    indexAndElement
  }

  override def save(key: Long, value: IElement, output: DataOutputStream): Unit = {
    val sb = new StringBuilder

    // Write node id
    sb.append(key)
    sb.append(keyModSep)

    // Write feats
    val walkPath = value.asInstanceOf[WalkPath]
    val mod = walkPath.getMod
    sb.append(mod)
    sb.append(keyValueSep)

    val feats = walkPath.getPath

    var index = 0
    val len = feats.length
    feats.foreach(f => {
      if (index < len - 1) {
        sb.append(f).append(featSep)
      } else {
        sb.append(f).append("\n")
      }
      index += 1
    })

    output.writeBytes(sb.toString())
  }
}
