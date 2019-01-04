package com.tencent.angel.spark.ml.embedding.sampler;

import scala.Tuple3;

import java.util.Iterator;
import java.util.Random;

/**
 * generate word-context pairs for Skip-Gram with negative sampling
 * <src, dst, label>
 */
public abstract class SampleGenerator implements Iterator<Tuple3<Integer, Integer, Integer>> {
    int[][] sentences;
    int seed;
    Random random;
    Random negRandom;
    final int neg; // number of negative samples
    final int numNode; //the max vertex ID

    int curNegId = 0; // counting negative samples
    int curSenId = 0; // counting sentences

    /**
     * @param sentences: the input random walks
     * @param seed: [seed] for random generator, [seed + 1] for negative sampling
     * @param numNode: number of vertices in the graph
     * @param neg: number of negative sampling for each node pair
     */
    public SampleGenerator(int[][] sentences, int seed, int numNode, int neg){
        this.sentences = sentences;
        this.seed = seed;
        this.numNode = numNode;
        this.neg = neg;
        random = new Random(seed);
        negRandom = new Random(seed + 1);
    }

    @Override
    public boolean hasNext() {
        return curSenId < sentences.length || curNegId < neg;
    }
}

