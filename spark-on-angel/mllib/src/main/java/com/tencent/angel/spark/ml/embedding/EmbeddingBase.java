package com.tencent.angel.spark.ml.embedding;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import scala.Tuple2;

abstract class EmbeddingBase {

    protected int window;
    protected int negative;
    protected float alpha;
    protected int numNode;
    protected int dimension;


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
    abstract int[] buildIndices(int [][] sentences, int seed);

    // train and
    abstract Tuple2<Double, Integer> train(int[][] sentences, int seed, float[] layers,
                                           Int2IntOpenHashMap index, float[] deltas);
}
