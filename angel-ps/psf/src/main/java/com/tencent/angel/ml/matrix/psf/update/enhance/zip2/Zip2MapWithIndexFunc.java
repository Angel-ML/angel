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

package com.tencent.angel.ml.matrix.psf.update.enhance.zip2;

import com.tencent.angel.common.Serialize;

/**
 * `Zip2MapWithIndexFunc` is a parameter of [[Zip2MapWithIndex]], if you want to  call a
 * `Zip2MapWithIndex` for a row in matrix, you must implement a `Zip2MapWithIndexFunc` as
 * parameter of `Zip2MapWithIndex`
 */
public interface Zip2MapWithIndexFunc extends Serialize {
   double call(long index, double value1, double value2);
}
