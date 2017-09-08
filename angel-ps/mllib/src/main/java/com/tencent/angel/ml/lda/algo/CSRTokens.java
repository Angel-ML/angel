package com.tencent.angel.ml.lda.algo;


import com.tencent.angel.ml.lda.algo.structures.S2BTraverseMap;
import com.tencent.angel.ml.lda.algo.structures.S2ITraverseMap;
import com.tencent.angel.ml.lda.algo.structures.S2STraverseMap;
import com.tencent.angel.ml.lda.algo.structures.TraverseHashMap;
import com.tencent.angel.worker.storage.Reader;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.List;

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

  public void build(List<Document> docs, int K) {
    int[] wcnt = new int[n_words];
    this.ws = new int[n_words + 1];
    docLens = new int[n_docs];
    docIds  = new long[n_docs];
    n_tokens = 0;

    // count word
    for (int d = 0; d < n_docs; d ++) {
      Document doc = docs.get(d);
      for (int w = 0; w < doc.len; w ++)
        wcnt[doc.wids[w]] ++;
      n_tokens += doc.len;
      docLens[d] = doc.len;
      docIds[d] = doc.docId;
    }


    this.docs = new int[n_tokens];
    this.topics = new int[n_tokens];

    // build word start index
    ws[0] = 0;
    for (int i = 0; i < n_words; i ++)
      ws[i + 1] = ws[i] + wcnt[i];

    for (int d = n_docs - 1; d >= 0; d --) {
      Document doc = docs.get(d);
      for (int w = 0; w < doc.len; w ++) {
        int wid = doc.wids[w];
        int pos = ws[wid] + (--wcnt[wid]);
        this.docs[pos] = d;
      }
    }

    // build dks
    dks = new TraverseHashMap[n_docs];
    for (int d = 0; d < n_docs; d ++) {
      if (docs.get(d).len < Byte.MAX_VALUE)
        dks[d] = new S2BTraverseMap(docs.get(d).len);
      if (docs.get(d).len < Short.MAX_VALUE)
        dks[d] = new S2STraverseMap(Math.min(K,
                docs.get(d).len));
      else
        dks[d] = new S2ITraverseMap(Math.min(K,
                docs.get(d).len));
    }
  }

  public static CSRTokens read(TaskContext ctx, int V, int K) throws Exception {
    // Read documents
    Reader<LongWritable, Text> reader = ctx.getReader();

    List<Document> docs = new ArrayList();

    while (reader.nextKeyValue())
      docs.add(new Document(reader.getCurrentValue().toString()));

    CSRTokens data = new CSRTokens(V, docs.size());
    data.build(docs, K);
    docs.clear();
    return data;
  }

}
