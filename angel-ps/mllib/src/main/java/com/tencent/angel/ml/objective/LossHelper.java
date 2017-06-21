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
package com.tencent.angel.ml.objective;

/**
 * Description: interface of loss
 */
public interface LossHelper {

  float transPred(float x);

  boolean checkLabel(float x);

  float firOrderGrad(float pred, float label);

  float secOrderGrad(float pred, float label);

  float prob2Margin(float baseScore);

  String labelErrorMsg();

  String defaultEvalMetric();

}