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

package com.tencent.angel.serving.common

import com.tencent.angel.serving.ServingLocation

import scala.collection.mutable

/**
  * the serving replica
  */
class ServingReplica {
  private[this] val _locations: mutable.HashSet[ServingLocation] = new mutable.HashSet[ServingLocation]()

  def locations: Array[ServingLocation] = {
    _locations.toArray
  }

  def addLoc(servingLoc: ServingLocation): Boolean = {
    synchronized {
      _locations.add(servingLoc)
    }
  }

  def removeLoc(servingLoc: ServingLocation): Boolean = {
    synchronized {
      _locations.remove(servingLoc)
    }
  }

  def cleanLoc(): Unit = {
    synchronized {
      _locations.clear()
    }
  }

  def diff(replica: Int): Int = {
    _locations.size - replica
  }

  def remove(replica: Int): Array[ServingLocation] = {
    synchronized {
      val taken = _locations.take(replica)
      taken.foreach(_ => _locations.remove(_))
      taken.toArray
    }
  }

  def renew(servingLocs: Array[ServingLocation]): Unit = {
    synchronized {
      val removed = locations.filter(loc => !servingLocs.contains(loc))
      servingLocs.foreach(addLoc(_))
      removed.foreach(removeLoc(_))
    }
  }

  override def toString: String = {
    _locations.mkString("(", "|", ")")
  }
}
