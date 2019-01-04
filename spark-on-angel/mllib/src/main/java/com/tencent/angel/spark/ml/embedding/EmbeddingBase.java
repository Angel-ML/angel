package com.tencent.angel.spark.ml.embedding;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import scala.Tuple2;

public abstract class EmbeddingBase {

    public int window;
    public int negative;
    public float alpha;
    public int numNode;
    public int dimension;


    public EmbeddingBase(int window, int negative, float alpha, int numNode, int dimension) {
        this.window = window;
        this.negative = negative;
        this.alpha = alpha;
        this.numNode = numNode;
        this.dimension = dimension;
    }

    /**
     * pre-scan the sentences and get the needed indices for embedding;
     * @param sentences
     * @param seed [seed] for word2vec dynamic window, [seed + 1] for negative sampling
     * @return
     */
    public abstract int[] buildIndices(int [][] sentences, int seed);

    /**
     *
     * @param sentences the random walks
     * @param seed the seed for generating the corpus
     * @param layers the pulled vectors. For each word, the word-vector and context-vector are both pulled.
     *               TODO: only pull one of them.
     * @param index map(vertexId, index)
     * @param deltas the deltas to be pushed to PS.
     * @return (sum_loss, loss_cnt)
     */
    public abstract Tuple2<Double, Integer> train(int[][] sentences, int seed, float[] layers,
                                           Int2IntOpenHashMap index, float[] deltas);
}
