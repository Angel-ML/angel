package com.tencent.angel.spark.ml.embedding;

import scala.Tuple3;

import java.util.Iterator;
import java.util.Random;

/**
 * generate word-context pairs for Skip-Gram with negative sampling
 * <src, dst, label>
 */
abstract class SampleGenerator implements Iterator<Tuple3<Integer, Integer, Integer>> {
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
class RootedPageRankSampler extends SampleGenerator{

    final float endPrp;
    public RootedPageRankSampler(int [][] sentences, int seed, int numNode, int neg){
        super(sentences, seed, numNode, neg);
        this.endPrp = 0.5f;
    }
    public RootedPageRankSampler(int [][] sentences, int seed, int numNode, int neg, float endPrp){
        super(sentences, seed, numNode, neg);
        this.endPrp = endPrp;
    }

    @Override
    public Tuple3<Integer, Integer, Integer> next() {
        int sen_len = sentences[curSenId].length;
        int srcId = random.nextInt(sen_len);
        if(curNegId == 0){ // sample positive pairs
            int dstId = srcId + 1;
            while(random.nextFloat() < endPrp && dstId < sen_len){
                dstId ++;
            }
            if(dstId < sen_len){
                curNegId ++;
                return new Tuple3<>(sentences[curSenId][srcId], sentences[curSenId][dstId], 1);
            }
            else{
                // no vertex pairs for this random walk path.
                // thus also no negative pairs for this sentences.
                curNegId = 0;
                curSenId += 1;
                return next();
            }
        }
        else if (curNegId <= neg){ // sample negative pairs
            int dst;
            do {
                dst = negRandom.nextInt(numNode);
            } while (dst == sentences[curSenId][srcId]);
            curNegId ++;
            return new Tuple3<>(sentences[curSenId][srcId], dst, 0);
        }
        else{ // jump to a new sentence
            curNegId = 0;
            curSenId ++;
            return next();
        }

    }
}
