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

package com.tencent.angel.ml.lda.algo;


import com.tencent.angel.ml.lda.algo.structures.S2BTraverseMap;
import com.tencent.angel.ml.lda.algo.structures.S2ITraverseMap;
import com.tencent.angel.ml.lda.algo.structures.S2STraverseMap;
import com.tencent.angel.ml.lda.algo.structures.TraverseHashMap;
import com.tencent.angel.worker.storage.DataBlock;

import java.io.IOException;

public class CSRTokens {
  public int n_words;
  public int n_docs;
  public int n_tokens;

  // start row index for words
  public int[] ws;
  // doc ids
  public int[] docs;
  // topic assignments
  public int[] topics;

  public TraverseHashMap[] dks;
  public int[] docLens;
  public long[] docIds;

  public CSRTokens(int n_words, int n_docs) {
    this.n_words = n_words;
    this.n_docs = n_docs;
  }

  public CSRTokens build(DataBlock<Document> docs, int K) throws IOException {
    int[] wcnt = new int[n_words];
    this.ws = new int[n_words + 1];
    docLens = new int[n_docs];
    docIds = new long[n_docs];
    n_tokens = 0;

    // count word
    for (int d = 0; d < n_docs; d++) {
      Document doc = docs.get(d);
      for (int w = 0; w < doc.len; w++)
        wcnt[doc.wids[w]]++;
      n_tokens += doc.len;
      docLens[d] = doc.len;
      docIds[d] = doc.docId;
    }


    this.docs = new int[n_tokens];
    this.topics = new int[n_tokens];

    // build word start index
    ws[0] = 0;
    for (int i = 0; i < n_words; i++)
      ws[i + 1] = ws[i] + wcnt[i];

    for (int d = n_docs - 1; d >= 0; d--) {
      Document doc = docs.get(d);
      for (int w = 0; w < doc.len; w++) {
        int wid = doc.wids[w];
        int pos = ws[wid] + (--wcnt[wid]);
        this.docs[pos] = d;
      }
    }

    // build dks
    dks = new TraverseHashMap[n_docs];
    for (int d = 0; d < n_docs; d++) {
      if (docs.get(d).len < Byte.MAX_VALUE)
        dks[d] = new S2BTraverseMap(docs.get(d).len);
      if (docs.get(d).len < Short.MAX_VALUE)
        dks[d] = new S2STraverseMap(Math.min(K, docs.get(d).len));
      else
        dks[d] = new S2ITraverseMap(Math.min(K, docs.get(d).len));
    }

    return this;
  }



}
