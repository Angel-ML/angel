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

package com.tencent.angel.spark.ml.gbt

object Utils {

  def findFIdPosition(featSet: Array[Int], fId: Int): Int = {
    // featSet is sorted
    if (fId < featSet(0) || fId > featSet(featSet.length - 1)) {
      return -1
    }

    var begin = 0
    var end = featSet.length - 1

    while(end >= begin) {
      val mid = (end + begin) / 2
      if (featSet(mid) == fId) {
        return mid
      } else if (featSet(mid) > fId) {
        end = mid - 1
      } else {
        begin = mid + 1
      }
    }
    -1
  }

  def findFValuePosition(sketch: Array[Double], fValue: Double): Int = {
    // sketch(0) is the minimal value of this feature, ignore the first sketch
    (1 until sketch.length).foreach { index =>
      if (sketch(index) >= fValue) {
        return index - 1
      }
    }
    sketch.length - 1
  }
}
