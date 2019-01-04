package com.tencent.angel.spark.ml.embedding;

import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import scala.Int;
import scala.Tuple2;
import scala.Tuple3;
import sun.awt.geom.AreaOp;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Random;

public class SGNSModel extends EmbeddingBase{
    private float endPrp = 0.5f; // end probablity for rooted pagerank
    private SampleGenerator sampleGenerator;

    public SGNSModel(int window, int negative, float alpha, int numNode, int dimension) {
        super(window, negative, alpha, numNode, dimension);
    }

    @Override
    int[] buildIndices(int[][] sentences, int seed) {
        return new int[0];
    }

    /**
     * extract the node-pairs for embedding, implementations can be rooted pagerank,
     * pure random walk, etc.
     * @param sentences
     * @param seed
     * @return [src, dst, src, dst, src, dst, ...]
     */
    public int[] pairsForSGNS(int [][] sentences, int seed){
        String methodName = "RPR";
        ArrayList<Integer> result = new ArrayList<Integer>(10000);
        int src, dst;
        Random random = new Random(seed);

        for(int s = 0; s < sentences.length; s ++){
            int[] curSen = sentences[s];
            switch (methodName){
                case "RPR": // do rooted pagerank
                    src = curSen[0];
                    dst = 1;
                    while(random.nextFloat() < endPrp) {dst ++;}
                    result.add(src);
                    result.add(curSen[dst]);
                    break;
                default: // pure random walk without dynamic windows
                    src = curSen[0];
                    dst = 1;
                    while(dst < curSen.length){
                        result.add(src);
                        result.add(curSen[dst]);
                        dst += 1;
                    }
            }
        }

        // parse arraylist<Integer> to int[]
        Iterator<Integer> iter = result.iterator();
        int intResult[] = new int[result.size()];
        int i = 0;
        while(iter.hasNext()){
            intResult[i] = iter.next();
            i ++;
        }

        return intResult;
    }

    public int[] buildIndices(int pairs[], int seed){
        RootedPageRankSampler rpr = new RootedPageRankSampler()
        Random negRandom = new Random(seed);
        IntOpenHashSet indices = new IntOpenHashSet();
        for(int i = 0; i < pairs.length; i += 2){
            indices.add(pairs[i]);
            indices.add(pairs[i + 1]);
            for(int j = 0; j < negative; j++){
                int target;
                do{
                    target = negRandom.nextInt(numNode);
                }while (target == pairs[i]);
                indices.add(target);
            }

        }
        return indices.toIntArray();
    }


//  public int[] indicesForCbow(int[][] sentences, int seed) {
//    Random winRand = new Random(seed);
//    Random negRand = new Random(seed + 1);
//
//    IntOpenHashSet indices = new IntOpenHashSet();
//    for (int s = 0; s < sentences.length; s++) {
//      int len = sentences[s].length;
//      for (int p = 0; p < len; p++) {
//        int word = sentences[s][p];
//        indices.add(word);
//
//        int b = winRand.nextInt(window);
//
//        int cw = 0;
//
//        for (int a = b; a < window * 2 + 1 - b; a++) {
//          if (a != window) {
//            int c = p - window + a;
//            if (c < 0) continue;
//            if (c > len) continue;
//            cw ++;
//          }
//        }
//
//        if (cw > 0) {
//          int target;
//          for (int d = 0; d < negative; d++) {
//            do {
//              target = negRand.nextInt(numNode);
//            } while (target == word);
//            indices.add(target);
//          }
//        }
//      }
//    }
//    return indices.toIntArray();
//  }

    public Tuple2<Double, Integer> train(int[][] sentences, int seed, float[] layers, Int2IntOpenHashMap index, float[] deltas) {

        int[] embeddingPairs = pairsForSGNS(sentences, seed);
        Arrays.fill(deltas, 0.0f);

        Random winRand = new Random(seed);
        Random negRand = new Random(seed + 1);

        Int2IntOpenHashMap inputCounter = new Int2IntOpenHashMap();
        Int2IntOpenHashMap outputCounter = new Int2IntOpenHashMap();

        float[] neu1 = new float[dimension];
        float[] neu1e = new float[dimension];
        int loss_cnt = 0;

        double sum_loss = 0.0;
        for (int s = 0; s < sentences.length; s++) {
            int sentence_length = sentences[s].length;

            for (int sentence_position = 0; sentence_position < sentence_length; sentence_position++) {
                // current word
                int word = sentences[s][sentence_position];
                // neu1 stores the average value of input vectors in the context (CBOW)
                Arrays.fill(neu1, 0);
                Arrays.fill(neu1e, 0);

                int b = winRand.nextInt(window);

                // Continuous Bag-of-Words Model
                int cw = 0;

                // Accumulate the input vectors from context
                for (int a = b; a < window * 2 + 1 - b; a++)
                    if (a != window) {
                        int c = sentence_position - window + a;
                        if (c < 0) continue;
                        if (c >= sentence_length) continue;
                        int last_word = sentences[s][c];
                        if (last_word == -1) continue;
                        if (!index.containsKey(last_word)) System.out.println("Error");
                        int offset = index.get(last_word) * dimension * 2;
                        for (c = 0; c < dimension; c++) neu1[c] += layers[c + offset];
                        cw++;
                    }

                if (cw > 0) {
                    for (int c = 0; c < dimension; c++) neu1[c] /= cw;
                    // negative sampling
                    int target, label;
                    for (int d = 0; d < negative + 1; d++) {
                        if (d == 0) {
                            target = word;
                            label = 1;
                        } else {
                            do {
                                target = negRand.nextInt(numNode);
                            } while (target == word);
                            label = 0;
                        }

                        if (!index.containsKey(target)) System.out.println("Error");
                        int l2 = index.get(target) * dimension * 2 + dimension;
                        float f = 0f;
                        for (int c = 0; c < dimension; c++) f += neu1[c] * layers[c + l2];

                        float prob = FastSigmoid.sigmoid(f);
                        if (d == 0) sum_loss -= FastSigmoid.log(prob);
                        else sum_loss -= FastSigmoid.log(1 - prob);
                        loss_cnt ++;


                        // Using the sigmoid value from the pre-computed table
                        float g = (label - FastSigmoid.sigmoid(f)) * alpha;
                        // accumulate for the hidden layer
                        for (int c = 0; c < dimension; c++) neu1e[c] += g * layers[c + l2];
                        // update output layer
                        for (int c = 0; c < dimension; c++) deltas[l2 + c] += g * neu1[c];
                        // add the counter for target
                        outputCounter.addTo(target, 1);
                    }

                    // update hidden layer
                    for (int a = b; a < window * 2 + 1 - b; a++)
                        if (a != window) {
                            int c = sentence_position - window + a;
                            if (c < 0) continue;
                            if (c >= sentence_length) continue;
                            int last_word = sentences[s][c];
                            if (last_word == -1) continue;
                            // Update the input vector for each word in the context
                            int offset = index.get(last_word) * dimension * 2;
                            for (c = 0; c < dimension; c++) deltas[c + offset] += neu1e[c];
                            // add the counter to input
                            inputCounter.addTo(last_word, 1);
                        }
                }
            }
        }


        // update input layers
        ObjectIterator<Int2IntMap.Entry> it = inputCounter.int2IntEntrySet().fastIterator();
        while (it.hasNext()) {
            Int2IntMap.Entry entry = it.next();
            int node = entry.getIntKey();
            int offset = index.get(node) * dimension * 2;
            int divider = entry.getIntValue();
            for (int a = 0; a < dimension; a++) deltas[a + offset] /=  divider;
        }

        // update output layers
        it = outputCounter.int2IntEntrySet().fastIterator();
        while (it.hasNext()) {
            Int2IntMap.Entry entry = it.next();
            int node = entry.getIntKey();
            int offset = index.get(node) * dimension * 2 + dimension;
            int divider = entry.getIntValue();
            for (int a = 0; a < dimension; a++) deltas[a + offset] /=  divider;
        }

//    for (int a = 0; a < layers.length; a++) deltas[a] = layers[a] - deltas[a];
        return new Tuple2<>(sum_loss, loss_cnt);
    }

}

