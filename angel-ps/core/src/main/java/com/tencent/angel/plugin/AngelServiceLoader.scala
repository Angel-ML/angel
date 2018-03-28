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

package com.tencent.angel.plugin


import java.util.ServiceLoader

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.utils.ReflectionUtils
import org.apache.commons.logging.LogFactory
import org.apache.hadoop.conf.Configuration

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.util.Try


class AngelServiceLoader {

}

object AngelServiceLoader {

  val LOG = LogFactory.getLog(classOf[AngelServiceLoader])

  final private[this] var services: ArrayBuffer[AngelService[_]] = new ArrayBuffer[AngelService[_]]()


  @volatile
  private var loaded: Boolean = false

  private val mutex = new Object()
  private val serviceLoader = ServiceLoader.load(classOf[AngelService[_]])
  private val serviceClasses = new mutable.HashSet[Class[_]]()

  private def loadServices(): Unit = {
    mutex.synchronized {
      if (!loaded) {
        val iterator = serviceLoader.iterator()
        while (iterator.hasNext) {
          val a = iterator.next()
          serviceClasses.add(a.getClass)
        }
      }
      loaded = true
    }
  }

  def initServices(serviceClass: Class[AngelService[_]], conf: Configuration): AngelService[_] = {
    ReflectionUtils.newInstance(serviceClass, conf)
  }

  def startServiceIfNeed[T](t: T, conf: Configuration): Unit = {
    if (conf.getBoolean(AngelConf.ANGEL_PLUGIN_SERVICE_ENABLE, false)) {
      AngelServiceLoader.loadServices()
      val tmpServices = serviceClasses.map(clazz =>
        initServices(clazz.asInstanceOf[Class[AngelService[_]]], conf).asInstanceOf[AngelService[T]]
      ).filter(_.check(t)).toArray
      tmpServices.foreach(service => Try({
        service.start(t)
        LOG.info(s" start plugin service:$service")
      }).failed.foreach(throwable => {
        LOG.error(s"failed start plugin service:$service", throwable)
      }))
      services ++= tmpServices.asInstanceOf[Array[AngelService[_]]]
    }
  }

  def stopService(): Unit = {
    if (services != null) {
      services.foreach(service => Try({
        service.stop()
        LOG.info(s" stop plugin service:$service")
      }).failed.foreach(throwable => {
        LOG.error(s"failed start plugin service:$service", throwable)
      }))
    }
  }


}
