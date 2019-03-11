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

package com.tencent.angel.spark.ml.psf.embedding;

import com.tencent.angel.ml.matrix.psf.update.base.PartitionUpdateParam;
import com.tencent.angel.ml.matrix.psf.update.base.UpdateFunc;

public class Init extends UpdateFunc {

  public Init(InitParam param) {
    super(param);
  }

  public Init() { super(null);}

  @Override
  public void partitionUpdate(PartitionUpdateParam partParam) {
    if (partParam instanceof InitPartitionParam) {

      InitPartitionParam param = (InitPartitionParam) partParam;
      ServerWrapper.initialize(param.numPartitions, param.maxIndex,
          param.maxLength, param.negative, param.order, param.partDim, param.window);
    }
  }
}
