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
package com.tencent.angel.ml.core.utils

import scala.reflect.ClassTag

sealed trait TransLabel extends Serializable {
  def trans(label: Double): Double
}

class NoTrans extends TransLabel{
  override def trans(label: Double): Double = label
}

class PosNegTrans(threshold: Double = 0.5) extends TransLabel{
  override def trans(label: Double): Double = {
    if (label > threshold) 1.0 else - 1.0
  }
}

class ZeroOneTrans(threshold: Double = 0.5) extends TransLabel{
  override def trans(label: Double): Double = {
    if (label > threshold) 1.0 else 0.0
  }
}

class AddOneTrans extends TransLabel {
  override def trans(label: Double): Double = label + 1.0
}

class SubOneTrans extends TransLabel {
  override def trans(label: Double): Double = label - 1.0
}


object TransLabel {
  private def matchClass[T: ClassTag](name: String): Boolean = {
    val clz = implicitly[ClassTag[T]].runtimeClass
    clz.getSimpleName.equalsIgnoreCase(name)
  }

  def get(name: String, threshold: Double = 0): TransLabel = {
    name match {
      case clzName if matchClass[NoTrans](clzName) => new NoTrans()
      case clzName if matchClass[PosNegTrans](clzName) => new PosNegTrans(threshold)
      case clzName if matchClass[ZeroOneTrans](clzName) => new ZeroOneTrans(threshold)
      case clzName if matchClass[AddOneTrans](clzName) => new AddOneTrans()
      case clzName if matchClass[SubOneTrans](clzName) => new SubOneTrans()
    }
  }
}


