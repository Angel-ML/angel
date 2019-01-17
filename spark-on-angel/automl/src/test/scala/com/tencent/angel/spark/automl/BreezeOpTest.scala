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


package com.tencent.angel.spark.automl

import com.tencent.angel.spark.automl.tuner.math.BreezeOp._
import org.junit.Assert._
import org.junit._

class BreezeOpTest {

  @Test def test_cartesian = {

    val a: Array[Double] = Array(1.0, 2.0)
    val b: Array[Double] = Array(3.0, 4.0)
    val c: Array[Array[Double]] = cartesian(a, b)
//    println(c.deep.mkString("\n"))
    val expected:Array[Array[Double]] = Array(Array(1.0, 3.0), Array(1.0, 4.0), Array(2.0, 3.0), Array(2.0, 4.0))
//    println(expected.deep.mkString("\n"))
    assertEquals(expected.deep.mkString("\n"), c.deep.mkString("\n"))
  }

  @Test def test_cartesian_ = {

    val a: Array[Double] = Array(1.0, 2.0)
    val b: Array[Double] = Array(3.0, 4.0)
    val c: Array[Double] = Array(5.0, 6.0)
    val d: Array[Array[Double]] = cartesian(a, b)
    val e: Array[Array[Double]] = cartesian(d,c)
    val expected = Array(Array(1.0, 3.0, 5.0),
      Array(1.0, 3.0, 6.0),
      Array(1.0, 4.0, 5.0),
      Array(1.0, 4.0, 6.0),
      Array(2.0, 3.0, 5.0),
      Array(2.0, 3.0, 6.0),
      Array(2.0, 4.0, 5.0),
      Array(2.0, 4.0, 6.0))
    println(e.deep.mkString("\n"))
    assertEquals(expected.deep.mkString("\n"), e.deep.mkString("\n"))
  }

  @Test def test_cartesian_Array = {

    val a: Array[Double] = Array(1.0, 2.0)
    val b: Array[Double] = Array(3.0, 4.0)
    val c: Array[Double] = Array(5.0, 6.0)
    val d: Array[Double] = Array(7.0, 8.0)
    val params_array = Array(a,b,c,d)
    var tmp:Array[Array[Double]] = cartesian(params_array(0),params_array(1))
    params_array.foreach{case(a)=>
        if (a != params_array(0) && a != params_array(1)){
          tmp = cartesian(tmp,a)
        }
    }
    val expected = Array(Array(1.0, 3.0, 5.0, 7.0),
      Array(1.0, 3.0, 5.0, 8.0),
      Array(1.0, 3.0, 6.0, 7.0),
      Array(1.0, 3.0, 6.0, 8.0),
      Array(1.0, 4.0, 5.0, 7.0),
      Array(1.0, 4.0, 5.0, 8.0),
      Array(1.0, 4.0, 6.0, 7.0),
      Array(1.0, 4.0, 6.0, 8.0),
      Array(2.0, 3.0, 5.0, 7.0),
      Array(2.0, 3.0, 5.0, 8.0),
      Array(2.0, 3.0, 6.0, 7.0),
      Array(2.0, 3.0, 6.0, 8.0),
      Array(2.0, 4.0, 5.0, 7.0),
      Array(2.0, 4.0, 5.0, 8.0),
      Array(2.0, 4.0, 6.0, 7.0),
      Array(2.0, 4.0, 6.0, 8.0))
    println(tmp.deep.mkString("\n"))
    assertEquals(expected.deep.mkString("\n"), tmp.deep.mkString("\n"))
  }
}
