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


package com.tencent.angel.spark.ml.tree.gbdt.tree;

import com.tencent.angel.spark.ml.tree.gbdt.histogram.GradPair;
import com.tencent.angel.spark.ml.tree.tree.split.SplitEntry;
import java.io.Serializable;

public class GBTSplit implements Serializable {

  private SplitEntry splitEntry;
  private GradPair leftGradPair;  // grad pair of left child
  private GradPair rightGradPair; // grad pair of right child

  public GBTSplit() {
    this(null, null, null);
  }

  public GBTSplit(SplitEntry splitEntry, GradPair leftGradPair, GradPair rightGradPair) {
    super();
    this.splitEntry = splitEntry;
    this.leftGradPair = leftGradPair;
    this.rightGradPair = rightGradPair;
  }

  public boolean isValid(float minSplitGain) {
    return splitEntry != null && !splitEntry.isEmpty()
        && splitEntry.getGain() > minSplitGain;
  }

  public boolean needReplace(GBTSplit split) {
    if (this.splitEntry != null) {
      return split.splitEntry != null && this.splitEntry.needReplace(split.splitEntry);
    } else {
      return split.splitEntry != null;
    }
  }

  public void update(GBTSplit split) {
    if (this.needReplace(split)) {
      this.splitEntry = split.splitEntry;
      this.leftGradPair = split.leftGradPair;
      this.rightGradPair = split.rightGradPair;
    }
  }

  public SplitEntry getSplitEntry() {
    return splitEntry;
  }

  public void setSplitEntry(SplitEntry splitEntry) {
    this.splitEntry = splitEntry;
  }

  public GradPair getLeftGradPair() {
    return leftGradPair;
  }

  public GradPair getRightGradPair() {
    return rightGradPair;
  }
}
