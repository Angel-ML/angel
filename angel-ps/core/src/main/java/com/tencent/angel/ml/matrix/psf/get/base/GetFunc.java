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
 */

package com.tencent.angel.ml.matrix.psf.get.base;

import java.util.List;

/**
 * The base class of get udf function,invoke at parameter server actually. Please note: get udf use
 * the asynchronous consistency control protocol.
 */
public abstract class GetFunc {
  /** parameter of get udf */
  public final GetParam param;

  /**
   * Create a new DefaultGetFunc.
   *
   * @param param parameter of get udf
   */
  public GetFunc(GetParam param) {
    this.param = param;
  }

  /**
   * Gets parameter.
   *
   * @return the parameter
   */
  public GetParam getParam() {
    return param;
  }

  /**
   * Partition get. This function is called on PS.
   *
   * @param partParam the partition parameter
   * @return the partition result
   */
  public abstract PartitionGetResult partitionGet(PartitionGetParam partParam);

  /**
   * Merge the partition get results. This function is called on PSAgent.
   *
   * @param partResults the partition results
   * @return the merged result
   */
  public abstract GetResult merge(List<PartitionGetResult> partResults);
}
