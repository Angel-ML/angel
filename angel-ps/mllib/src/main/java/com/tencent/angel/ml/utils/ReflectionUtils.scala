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
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.utils

import com.tencent.angel.ml.conf.MLConf

object ReflectionUtils {

  import scala.reflect.runtime.{universe => ru}

  private lazy val rootMirror = ru.runtimeMirror(getClass.getClassLoader)

  def getAttr(item: String): String = {
    getCompanionAttr(item)
  }

  /**
    * Used for python code to get MLConf parameters, since MLConf.scala is a companion object,
    * we add MLConf class, although it contains nothing.
    *
    * @param item The fields that python code want to get
    * @param tt   Rutime TypeTag
    * @return The fields value
    */
  def getCompanionAttr(item: String)(implicit tt: ru.TypeTag[MLConf]): String = {

    val classMirror = rootMirror.reflectClass(tt.tpe.typeSymbol.asClass)
    val companionSymbol = classMirror.symbol.companion
    val companionInstance = rootMirror.reflectModule(companionSymbol.asModule)
    val companionMirror = rootMirror.reflect(companionInstance.instance)

    val fieldSymbol = companionSymbol.typeSignature.decl(ru.TermName(item)).asTerm
    val fieldMirror = companionMirror.reflectField(fieldSymbol)

    fieldMirror.get.asInstanceOf[String]
  }
}
