package com.tencent.angel.spark.ml.embedding.sampler;

import scala.Tuple3;

/**
 * Sample a word-context pair following rooted pagerank from the sampled random walks.
 * One word-context pair per sentence, where the word is sampled uniformly from the sentence and the context by
 * random walk
 */
public class RootedPageRankSampler extends SampleGenerator{

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
            while(random.nextFloat() > endPrp && dstId < sen_len){
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
                if(curSenId < sentences.length)
                    return next();
                else
                    return null;
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
            if(curSenId < sentences.length)
                return next();
            else
                return null;
        }

    }
}
