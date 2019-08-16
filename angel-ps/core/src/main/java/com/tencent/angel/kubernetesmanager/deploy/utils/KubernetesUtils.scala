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
package com.tencent.angel.kubernetesmanager.deploy.utils

import java.util.NoSuchElementException

import com.tencent.angel.conf.AngelConf
import com.tencent.angel.kubernetesmanager.deploy.config._
import org.apache.hadoop.conf.Configuration

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

private[angel] object KubernetesUtils {

  def parseMasterUrl(url: String): String = url.substring("k8s://".length)

  /**
    * Extract and parse Angel configuration properties with a given name prefix and
    * return the result as a Map. Keys must not have more than one value.
    *
    * @param angelConf Angel configuration
    * @param prefix    the given property name prefix
    * @return a Map storing the configuration property keys and values
    */
  def parsePrefixedKeyValuePairs(
                                  angelConf: Configuration,
                                  prefix: String): Map[String, String] = {
    angelConf.iterator().asScala.map(x => (x.getKey, x.getValue)).toArray
      .filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }.toMap
  }

  /**
    * Extract Angel volume configuration properties with a given name prefix.
    *
    * @param angelConf Angel configuration
    * @param prefix    the given property name prefix
    * @return a Map storing with volume name as key and spec as value
    */
  def parseVolumesWithPrefix(
                              angelConf: Configuration,
                              prefix: String): Iterable[Try[KubernetesVolumeSpec[_ <: KubernetesVolumeSpecificConf]]] = {
    val properties = angelConf.iterator().asScala.map(x => (x.getKey, x.getValue)).toArray
      .filter { case (k, v) => k.startsWith(prefix) }
      .map { case (k, v) => (k.substring(prefix.length), v) }.toMap

    getVolumeTypesAndNames(properties).map { case (volumeType, volumeName) =>
      val pathKey = s"$volumeType.$volumeName." + AngelConf.ANGEL_KUBERNETES_VOLUMES_MOUNT_PATH_KEY
      val readOnlyKey = s"$volumeType.$volumeName." + AngelConf.ANGEL_KUBERNETES_VOLUMES_MOUNT_READONLY_KEY

      for {
        path <- properties.getTry(pathKey)
        volumeConf <- parseVolumeSpecificConf(properties, volumeType, volumeName)
      } yield KubernetesVolumeSpec(
        volumeName = volumeName,
        mountPath = path,
        mountReadOnly = properties.get(readOnlyKey).exists(_.toBoolean),
        volumeConf = volumeConf
      )
    }
  }

  /**
    * Get unique pairs of volumeType and volumeName,
    * assuming options are formatted in this way:
    * `volumeType`.`volumeName`.`property` = `value`
    *
    * @param properties flat mapping of property names to values
    * @return Set[(volumeType, volumeName)]
    */
  private def getVolumeTypesAndNames(
                                      properties: Map[String, String]
                                    ): Set[(String, String)] = {
    properties.keys.flatMap { k =>
      k.split('.').toList match {
        case tpe :: name :: _ => Some((tpe, name))
        case _ => None
      }
    }.toSet
  }

  private def parseVolumeSpecificConf(
                                       options: Map[String, String],
                                       volumeType: String,
                                       volumeName: String): Try[KubernetesVolumeSpecificConf] = {
    volumeType match {
      case AngelConf.ANGEL_KUBERNETES_VOLUMES_HOSTPATH_TYPE =>
        val pathKey = s"$volumeType.$volumeName." + AngelConf.ANGEL_KUBERNETES_VOLUMES_OPTIONS_PATH_KEY
        for {
          path <- options.getTry(pathKey)
        } yield KubernetesHostPathVolumeConf(path)

      case AngelConf.ANGEL_KUBERNETES_VOLUMES_PVC_TYPE =>
        val claimNameKey = s"$volumeType.$volumeName." + AngelConf.ANGEL_KUBERNETES_VOLUMES_OPTIONS_CLAIM_NAME_KEY
        for {
          claimName <- options.getTry(claimNameKey)
        } yield KubernetesPVCVolumeConf(claimName)

      case AngelConf.ANGEL_KUBERNETES_VOLUMES_EMPTYDIR_TYPE =>
        val mediumKey = s"$volumeType.$volumeName." + AngelConf.ANGEL_KUBERNETES_VOLUMES_OPTIONS_MEDIUM_KEY
        val sizeLimitKey = s"$volumeType.$volumeName." + AngelConf.ANGEL_KUBERNETES_VOLUMES_OPTIONS_SIZE_LIMIT_KEY
        Success(KubernetesEmptyDirVolumeConf(options.get(mediumKey), options.get(sizeLimitKey)))

      case _ =>
        Failure(new RuntimeException(s"Kubernetes Volume type `$volumeType` is not supported"))
    }
  }

  def requireNandDefined(opt1: Option[_], opt2: Option[_], errMessage: String): Unit = {
    opt1.foreach { _ => require(opt2.isEmpty, errMessage) }
  }

  /**
    * Convenience wrapper to accumulate key lookup errors
    */
  implicit private class MapOps[A, B](m: Map[A, B]) {
    def getTry(key: A): Try[B] = {
      m.get(key)
        .fold[Try[B]](Failure(new NoSuchElementException(key.toString)))(Success(_))
    }
  }

}
