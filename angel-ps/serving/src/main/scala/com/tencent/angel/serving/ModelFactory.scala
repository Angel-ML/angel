/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 *  Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 *  https://opensource.org/licenses/BSD-3-Clause
 *
 *  Unless required by applicable law or agreed to in writing, software distributed under the License
 *  is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 *  or implied. See the License for the specific language governing permissions and limitations under
 *  the License.
 *
 */

package com.tencent.angel.serving

import java.util.concurrent.ConcurrentHashMap

import com.tencent.angel.exception.AngelException
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.collection.concurrent

/**
  * the model instance factory, construct the [[ShardingModel]] by specified class
  */
object ModelFactory {

  val modelMap: concurrent.Map[String, Class[_ <: ShardingModel]] = new ConcurrentHashMap[String, Class[_ <: ShardingModel]]().asScala

  def register(name: String, clazz: Class[_ <: ShardingModel]): Unit = {
    require(modelMap.put(name, clazz).isEmpty)
  }

  def get[M <: ShardingModel](name: String, clazzName:String, classLoader: ClassLoader = Thread.currentThread().getContextClassLoader): Class[M] = {
    modelMap.getOrElseUpdate(name, {
      if (clazzName != null) {
        var clazz: Class[_] = null
        try {
          clazz = classLoader.loadClass(clazzName)
          require(classOf[ShardingModel].isAssignableFrom(clazz))
          clazz.asInstanceOf[Class[M]]
        } catch {
          case e: ClassNotFoundException => throw new AngelException(s"not found class for $name", e)
        }
      } else {
        throw new AngelException(s"not found class for $name")
      }
    }).asInstanceOf[Class[M]]
  }

  def init[M <: ShardingModel](clazz: Class[M], matrices: Map[String, ShardingMatrix]): M = {
    val model = clazz.newInstance()
    model.init(matrices)
    model.load()
    model
  }

}
