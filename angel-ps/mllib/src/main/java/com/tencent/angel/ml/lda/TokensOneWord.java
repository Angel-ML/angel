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

package com.tencent.angel.ml.lda;

import it.unimi.dsi.fastutil.ints.IntArrayList;

public class TokensOneWord {

  private int wordId;

  private int[] docIds;
  private short[] topics;

  public TokensOneWord(int wordId, int[] docIds) {
    this.docIds = docIds;
    this.wordId = wordId;
    this.topics = new short[docIds.length];
  }

  public TokensOneWord(int wordId, IntArrayList docIds) {
    this(wordId, docIds.toIntArray());
  }

  public int getWordId() {
    return wordId;
  }

  public int size() {
    return docIds.length;
  }

  public int getTopic(int index) {
    return topics[index];
  }

  public void setTopic(int index, int topic) {
    topics[index] = (short) topic;
  }

  public int getDocId(int index) {
    return docIds[index];
  }
}