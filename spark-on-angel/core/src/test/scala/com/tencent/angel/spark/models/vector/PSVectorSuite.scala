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

package com.tencent.angel.spark.models.vector

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.{AngelPSContext, PSContext}
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class PSVectorSuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psContext: PSContext = _
  private var _psVector: PSVector = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.instance()
    _psVector = PSVector.dense(dim, capacity)
  }

  override def afterAll(): Unit = {
    _psContext.destroyVectorPool(_psVector)
    super.afterAll()
  }

  test("toRemote") {
    val remoteVector = _psVector.toCache

    assert(PSClient.instance().vectorOps.pull(_psVector).sameElements(remoteVector.pullFromCache()))
  }

  test("toBreeze") {
    val brzVector = _psVector.toBreeze

    assert(PSClient.instance().vectorOps.pull(_psVector).sameElements(brzVector.pull()))
  }

  test("delete") {
    val pool = PSContext.instance().asInstanceOf[AngelPSContext]
      .getPool(_psVector.poolId)

    val poolSize = pool.size

    val ps = PSVector.duplicate(_psVector)

    assert(pool.size == poolSize + 1)

    ps.delete()

    assert(pool.size == poolSize)
  }

  test("duplicate Vector") {
    val dVector = PSVector.duplicate(_psVector)
    assert(dVector.poolId == _psVector.poolId)
    assert(dVector.id != _psVector.id)

  }

  test("new dense vector") {
    val dVector = PSVector.dense(dim, capacity)
    assert(dVector.poolId != _psVector.poolId)
  }

  /**
  *test("new sparse vector") {
    *val sVector = PSVector.sparse(dim, capacity)
    *assert(sVector.isInstanceOf[SparsePSVector])
  *}
   */

}
