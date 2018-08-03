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

package com.tencent.angel.ml.matrix;

import java.util.List;

/**
 * Matrix report to Master
 */
public class MatrixReport {
  /**
   * Matrix id
   */
  public final int matrixId;

  /**
   * Matrix partitions reports
   */
  public final List<PartReport> partReports;

  /**
   * Create a matrix report
   * @param matrixId matrix id
   * @param partReports matrix partitions reports
   */
  public MatrixReport(int matrixId, List<PartReport> partReports) {
    this.matrixId = matrixId;
    this.partReports = partReports;
  }
}
