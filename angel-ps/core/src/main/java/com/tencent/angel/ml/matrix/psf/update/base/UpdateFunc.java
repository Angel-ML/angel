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


package com.tencent.angel.ml.matrix.psf.update.base;

import com.tencent.angel.ps.PSContext;

/**
 * The base class updating function,invoke at parameter server actually. Please note: update udf use
 * the asynchronous consistency control protocol.
 */
public abstract class UpdateFunc {
  /**
   * parameter of get udf
   */
  private final UpdateParam param;

  /**
   * PS context
   */
  protected PSContext psContext;

  /**
   * Create a new UpdateParam
   *
   * @param param
   */
  public UpdateFunc(UpdateParam param) {
    this.param = param;
  }

  /**
   * Gets parameter.
   *
   * @return the updating
   */
  public UpdateParam getParam() {
    return param;
  }

  /**
   * Set PS context
   *
   * @param psContext ps context
   */
  public void setPsContext(PSContext psContext) {
    this.psContext = psContext;
  }

  /**
   * Partition update.
   *
   * @param partParam the partition parameter
   */
  public abstract void partitionUpdate(PartitionUpdateParam partParam);
}
