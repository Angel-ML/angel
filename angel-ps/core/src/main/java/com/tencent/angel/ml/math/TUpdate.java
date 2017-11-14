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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.math;


/**
 * The class represented a update element in PS.
 */
public abstract class TUpdate {

  /**
   * Gets matrix id.
   *
   * @return the matrix id
   */
  public abstract int getMatrixId();

  /**
   * Sets matrix id.
   *
   * @param matrixId the matrix id
   * @return a reference of current update
   */
  public abstract TUpdate setMatrixId(int matrixId);

}
