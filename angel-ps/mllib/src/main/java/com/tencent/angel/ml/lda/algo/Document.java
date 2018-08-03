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

package com.tencent.angel.ml.lda.algo;


public class Document {
  public long docId;
  public int len;
  public int[] wids;

  public Document(long docId, int[] wids) {
    this.docId = docId;
    this.len = wids.length;
    this.wids = wids;
  }

  public Document(String str) {
    if (str.length() == 0)
      return;

    String[] parts = str.split("\t");
    docId = Long.parseLong(parts[0]);
    String wordIds = parts[1];

    String[] splits = wordIds.split(" ");
    if (splits.length < 1)
      return;

    wids = new int[splits.length];
    for (int i = 0; i < splits.length; i++)
      wids[i] = Integer.parseInt(splits[i]);

    len = splits.length;
  }

  public int len() {
    return len;
  }
}
