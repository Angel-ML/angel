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


package com.tencent.angel.jarvis.utils

object TeslaParam {
  // system param keys
  val TESLA_CONF_PATH = "user-files"
  val MODEL_PATH = "modelPath"

  val ACTION_TYPE = "actionType"
  object ActionType {
    val TRAIN = "train"
    val PREDICT = "predict"
  }
}

object AngelParam {
  // system param keys
  val ANGEL_ACTION_TYPE = "action.type"
  val ANGEL_SAVE_MODEL_PATH = "angel.save.model.path"
  val ANGEL_LOAD_MODEL_PATH = "angel.load.model.path"
}