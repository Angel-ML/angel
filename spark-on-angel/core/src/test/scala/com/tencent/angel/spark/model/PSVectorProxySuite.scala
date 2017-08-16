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

package com.tencent.angel.spark.model

import com.tencent.angel.spark.client.PSClient
import com.tencent.angel.spark.context.PSContext
import com.tencent.angel.spark.{PSFunSuite, SharedPSContext}

class PSVectorProxySuite extends PSFunSuite with SharedPSContext {

  private val dim = 10
  private val capacity = 10
  private var _psContext: PSContext = _
  private var _pool: PSModelPool = _
  private var _psVectorProxy: PSModelProxy = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    _psContext = PSContext.getOrCreate()
    _pool = _psContext.createModelPool(dim, capacity)
    _psVectorProxy = _pool.createZero()
  }

  override def afterAll(): Unit = {
    _pool.delete(_psVectorProxy)
    _psContext.destroyModelPool(_pool)
    super.afterAll()
  }

  test("getPool") {
    val pool = _psVectorProxy.getPool()

    assert(pool.id == _pool.id)
  }


  test("mkRemote") {
    val remoteVector = _psVectorProxy.mkRemote()

    assert(PSClient().pull(_psVectorProxy).sameElements(remoteVector.pull()))
  }

  test("mkBreeze") {
    val brzVector = _psVectorProxy.mkBreeze()

    assert(PSClient().pull(_psVectorProxy).sameElements(brzVector.toRemote.pull()))
  }

  test("delete") {
    val psKey = _pool.createZero()
    psKey.assertValid()

    psKey.delete()
  }
}
