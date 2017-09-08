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
package com.tencent.angel.ml.GBDT.algo;


public class GBDTPhase {

  public static int CREATE_SKETCH = 0;
  public static int GET_SKETCH = 1;
  public static int NEW_TREE = 2;
  public static int RUN_ACTIVE = 3;
  public static int FIND_SPLIT = 4;
  public static int AFTER_SPLIT = 5;
  public static int FINISHED = 6;

}
