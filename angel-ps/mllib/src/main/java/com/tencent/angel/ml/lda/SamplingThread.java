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

import com.tencent.angel.ml.lda.structures.FTree;
import com.tencent.angel.ml.lda.structures.S2STraverseMap;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseIntVector;
import com.tencent.angel.ml.math.vector.SparseIntSortedVector;
import com.tencent.angel.ml.math.vector.TIntVector;
import com.tencent.angel.psagent.matrix.transport.adapter.GetRowsResult;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicIntegerArray;

public class SamplingThread implements Callable<Boolean> {

  private final static Log LOG = LogFactory.getLog(SamplingThread.class);

  private GetRowsResult taskQueue;
  private float[] p;
  private int[] topicIndex;
  private DenseIntVector topicUpdate;
  private LDAModel ldaModel;


  private FTree tree;
  private AtomicIntegerArray n_k;
  private Map<Integer, TokensOneWord> words;
  private S2STraverseMap[] n_dk;
  private List<Document> docs;

  private DenseIntVector local;

  public SamplingThread(LDAModel ldaModel,
                        AtomicIntegerArray n_k,
                        Map<Integer, TokensOneWord> words,
                        S2STraverseMap[] n_dk,
                        List<Document> docs) {
    this.n_k = n_k;
    this.words = words;
    this.docs = docs;
    this.n_dk = n_dk;
    this.ldaModel = ldaModel;

    p   = new float[ldaModel.K()];
    topicIndex = new int[ldaModel.K()];
    local = new DenseIntVector(ldaModel.K());
  }

  public void setTaskQueue(GetRowsResult taskQueue) {
    this.taskQueue = taskQueue;
  }

  @Override
  public Boolean call() throws Exception {
    TVector row;

    topicUpdate = new DenseIntVector(ldaModel.K());
    topicUpdate.setRowId(0);

    tree = new FTree(ldaModel.K());
    try {
      while (true) {
        row = taskQueue.poll();
        if (row == null && taskQueue.isFetchOver()) {
          break;
        }
        if (row == null) {
          Thread.sleep(10);
          continue;
        }
        int rowId = row.getRowId();
        DenseIntVector wtrow = format(row);
        buildFTree(wtrow, ldaModel.K(), ldaModel.beta(), ldaModel.vbeta(), tree, n_k);
        TokensOneWord list = words.get(rowId);
        DenseIntVector wordUpdate = sample(list, wtrow, docs, topicUpdate, topicIndex,
                p, tree, n_k, n_dk, ldaModel);

        ldaModel.wtMat().increment(wordUpdate);
      }
    } catch (Exception e) {
      LOG.error(e);
      e.printStackTrace();
    }

    ldaModel.tMat().increment(topicUpdate);

    return true;
  }

  private DenseIntVector format(TVector row) {
    if (row instanceof DenseIntVector) {
      return (DenseIntVector) row;
    } else if (row instanceof SparseIntSortedVector) {
      int[] keys = ((SparseIntSortedVector) row).getIndices();
      int[] values = ((SparseIntSortedVector) row).getValues();
      int[] dense = local.getValues();
      Arrays.fill(dense, 0);
      for (int i = 0; i < keys.length;i ++)
        dense[keys[i]] = values[i];
      local.setRowId(row.getRowId());
      local.setMatrixId(row.getMatrixId());
      return local;
    }
    return null;
  }

  public static void buildFTree(DenseIntVector row, int K, float beta, float vbeta,
                                FTree tree,
                                AtomicIntegerArray n_k) {
    for (int topic = 0; topic < K; topic ++) {
//      tree.tree[topic + K] = (row.get(topic) + beta) / (n_k.get(topic) + vbeta);
      tree.set(topic, (row.get(topic) + beta) / (n_k.get(topic) + vbeta) );
    }

    tree.build();
  }

  public void removeTopic(TIntVector row, int topic,
                          AtomicIntegerArray n_k,
                          S2STraverseMap map) {
    row.inc(topic, -1);
    n_k.addAndGet(topic, -1);
    map.dec(topic);
  }

  public void assignTopic(TIntVector row, int topic,
                                 AtomicIntegerArray n_k,
                                 S2STraverseMap map) {
    row.inc(topic, 1);
    n_k.addAndGet(topic, 1);
    map.inc(topic);
  }

  public float buildProArr(S2STraverseMap map, float[] p, int[] topicIndex, FTree tree) {
    float psum = 0.0F;
    short topic;
    short count;
    int idx = 0;
    for (int i = 0; i < map.size; i ++) {
      topic = map.key[map.idx[i]];
      count = map.value[map.idx[i]];
      psum += count * tree.get(topic);
      p[idx] = psum;
      topicIndex[idx ++] = topic;
    }
    return psum;
  }

  public DenseIntVector sample(TokensOneWord list,
                               DenseIntVector topicAssign,
                               List<Document> docs,
                               DenseIntVector topicUpdate,
                               int[] topicIndex,
                               float[] p,
                               FTree tree,
                               AtomicIntegerArray n_k,
                               S2STraverseMap[] n_dk,
                               LDAModel ldaModel) {
    Random random = new Random(System.currentTimeMillis());

    int length = list.size();
    int wordId = list.getWordId();

    DenseIntVector wordUpdate = new DenseIntVector(ldaModel.K());
    wordUpdate.setRowId(wordId);

    for (int i = 0; i < length; i++) {
      int docId = list.getDocId(i);
      int topic = list.getTopic(i);

      Document doc = docs.get(docId);

      int newTopic = -1;

      synchronized (doc) {
        removeTopic(topicAssign, topic, n_k, n_dk[docId]);
        tree.update(topic, (topicAssign.get(topic) + ldaModel.beta()) / (n_k.get(topic) + ldaModel.vbeta()));
        int size = n_dk[docId].size;
        float psum = buildProArr(n_dk[docId], p, topicIndex, tree);
        double u = random.nextFloat() * (psum + ldaModel.alpha() * tree.first());
        if (u < psum) {
          float u1 = random.nextFloat() * psum;
          int index = BinarySearch.binarySearch(p, u1, 0, size - 1);
          newTopic = topicIndex[index];
        } else {
          newTopic = tree.sample(random.nextFloat() * tree.first());
        }

        assignTopic(topicAssign, newTopic, n_k, n_dk[docId]);
        list.setTopic(i, newTopic);
        tree.update(newTopic, (topicAssign.get(newTopic) + ldaModel.beta()) / (n_k.get(newTopic) + ldaModel.vbeta()));
      }

      // Inc update to local buffers
      if (topic != newTopic) {
        wordUpdate.inc(topic, -1);
        wordUpdate.inc(newTopic, 1);

        topicUpdate.inc(topic, -1);
        topicUpdate.inc(newTopic, 1);
      }
    }

    return wordUpdate;
  }

}
