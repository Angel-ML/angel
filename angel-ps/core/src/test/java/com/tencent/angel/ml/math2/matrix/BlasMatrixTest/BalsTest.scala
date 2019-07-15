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

package com.tencent.angel.ml.math2.matrix.BlasMatrixTest

import com.tencent.angel.ml.math2.MFactory
import com.tencent.angel.ml.math2.matrix.{BlasDoubleMatrix, BlasFloatMatrix}
import com.tencent.angel.ml.math2.ufuncs.Ufuncs
import org.scalatest.FunSuite

class BalsTest extends FunSuite {
  val data1 = Array[Double](
    1.3, 2.7, 3.2, 5.1,
    3.0, 8.0, 9.5, 4.7,
    2.6, 8.3, 5.5, 8.9)

  val data2 = Array[Double](
    1.2, 5.7,
    3.1, 4.1,
    3.8, 3.5,
    8.4, 3.6)

  val data1f = Array[Float](
    1.3f, 2.7f, 3.2f, 5.1f,
    3.0f, 8.0f, 9.5f, 4.7f,
    2.6f, 8.3f, 5.5f, 8.9f)

  val data2f = Array[Float](
    1.2f, 5.7f,
    3.1f, 4.1f,
    3.8f, 3.5f,
    8.4f, 3.6f)

  test("Double-NN") {
    val mat1 = MFactory.denseDoubleMatrix(3, 4, data1)
    val mat2 = MFactory.denseDoubleMatrix(4, 2, data2)
    val res = Ufuncs.dot(mat1, false, mat2, false, true).asInstanceOf[BlasDoubleMatrix]

    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0
        (0 until 4).foreach { k =>
          tmp += data1(rId * 4 + k) * data2(2 * k + cId)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }

  }

  test("Double-NT") {
    val mat1 = MFactory.denseDoubleMatrix(3, 4, data1)
    val mat2 = MFactory.denseDoubleMatrix(2, 4, data2)
    val res = Ufuncs.dot(mat1, false, mat2, true, true).asInstanceOf[BlasDoubleMatrix]


    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0
        (0 until 4).foreach { k =>
          tmp += data1(rId * 4 + k) * data2(cId * 4 + k)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }
  }

  test("Double-TN") {
    val mat1 = MFactory.denseDoubleMatrix(4, 3, data1)
    val mat2 = MFactory.denseDoubleMatrix(4, 2, data2)
    val res = Ufuncs.dot(mat1, true, mat2, false, true).asInstanceOf[BlasDoubleMatrix]

    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0
        (0 until 4).foreach { k =>
          tmp += data1(k * 3 + rId) * data2(2 * k + cId)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }
  }

  test("Double-TT") {
    val mat1 = MFactory.denseDoubleMatrix(4, 3, data1)
    val mat2 = MFactory.denseDoubleMatrix(2, 4, data2)
    val res = Ufuncs.dot(mat1, true, mat2, true, true).asInstanceOf[BlasDoubleMatrix]

    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0
        (0 until 4).foreach { k =>
          tmp += data1(k * 3 + rId) * data2(cId * 4 +  k)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }
  }

  test("Float-NN") {
    val mat1 = MFactory.denseFloatMatrix(3, 4, data1f)
    val mat2 = MFactory.denseFloatMatrix(4, 2, data2f)
    val res = Ufuncs.dot(mat1, false, mat2, false, true).asInstanceOf[BlasFloatMatrix]

    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0f
        (0 until 4).foreach { k =>
          tmp += data1f(rId * 4 + k) * data2f(2 * k + cId)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }

  }

  test("Float-NT") {
    val mat1 = MFactory.denseFloatMatrix(3, 4, data1f)
    val mat2 = MFactory.denseFloatMatrix(2, 4, data2f)
    val res = Ufuncs.dot(mat1, false, mat2, true, true).asInstanceOf[BlasFloatMatrix]


    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0f
        (0 until 4).foreach { k =>
          tmp += data1f(rId * 4 + k) * data2f(cId * 4 + k)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }
  }

  test("Float-TN") {
    val mat1 = MFactory.denseFloatMatrix(4, 3, data1f)
    val mat2 = MFactory.denseFloatMatrix(4, 2, data2f)
    val res = Ufuncs.dot(mat1, true, mat2, false, true).asInstanceOf[BlasFloatMatrix]

    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0f
        (0 until 4).foreach { k =>
          tmp += data1f(k * 3 + rId) * data2f(2 * k + cId)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }
  }

  test("Float-TT") {
    val mat1 = MFactory.denseFloatMatrix(4, 3, data1f)
    val mat2 = MFactory.denseFloatMatrix(2, 4, data2f)
    val res = Ufuncs.dot(mat1, true, mat2, true, true).asInstanceOf[BlasFloatMatrix]

    (0 until 3).foreach { rId =>
      (0 until 2).foreach { cId =>
        var tmp = 0.0f
        (0 until 4).foreach { k =>
          tmp += data1f(k * 3 + rId) * data2f(cId * 4 +  k)
        }

        println(f"$tmp%.2f, ${res.get(rId, cId)}%.2f, ${tmp == res.get(rId, cId)}")
        assert(tmp == res.get(rId, cId))
      }
    }
  }
}
