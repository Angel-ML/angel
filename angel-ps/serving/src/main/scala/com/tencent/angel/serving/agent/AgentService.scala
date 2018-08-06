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

package com.tencent.angel.serving.agent

import com.tencent.angel.serving.common.ModelSplitID
import com.tencent.angel.serving.{PredictResult, PredictResultList, ShardingData}

/**
  * Serving service is a plugin-service on agent(such as parameter service)
  */
trait AgentService {

  /**
    * start serving service
    */
  def start(): Unit = ???

  /**
    * stop serving service
    */
  def stop(): Unit = ???

  /**
    * predict with sharding model
    *
    * @param modelSplitID the sharding model ID
    * @param shardingData the sharding predict data
    * @return the result
    */
  def predict(modelSplitID: ModelSplitID, shardingData: ShardingData): PredictResult

  /**
    * batch predict with sharding model
    *
    * @param modelSplitID
    * @param shardingData
    * @return the result list
    */
  def batchPredict(modelSplitID: ModelSplitID, shardingData: Array[ShardingData]): PredictResultList


}
