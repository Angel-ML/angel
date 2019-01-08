package com.tencent.angel.spark.ml.embedding;


import com.tencent.angel.spark.ml.embedding.sampler.RootedPageRankSampler;
import com.tencent.angel.spark.ml.embedding.sampler.SampleGenerator;
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import scala.Tuple2;
import scala.Tuple3;

public class SGNSModel extends EmbeddingBase {
    private float endPrp = 0.5f; // end probablity for rooted pagerank
    private SampleGenerator sampleGenerator;
    private String samplerName = "rootedPageRank";

    public void setSampler(String name) {
        this.samplerName = name;
    }

    public SGNSModel(int negative, float alpha, int numNode, int dimension, String samplerName, float endPrp) {
        super(negative, alpha, numNode, dimension);
        this.samplerName = samplerName;
        this.endPrp = endPrp;
    }

    @Override
    public int[] buildIndices(int[][] sentences, int seed) {
        switch (samplerName) {
            case "rootedPageRank":
                sampleGenerator = new RootedPageRankSampler(sentences, seed, numNode, negative, endPrp);
            default:
                sampleGenerator = new RootedPageRankSampler(sentences, seed, numNode, negative, endPrp);
        }
        IntOpenHashSet indices = new IntOpenHashSet();
        while (sampleGenerator.hasNext()) {
            Tuple3<Integer, Integer, Integer> tmp = sampleGenerator.next();
            if(tmp == null) break;

            indices.add(tmp._1()); // src
            indices.add(tmp._2()); // dst

        }
        return indices.toIntArray();
    }


    public Tuple2<Double, Integer> train(int[][] sentences, int seed, float[] layers,
                                         Int2IntOpenHashMap index, float[] deltas) {

        switch (samplerName) {
            case "rootedPageRank":
                sampleGenerator
                        = new RootedPageRankSampler(sentences, seed, numNode, negative);
            default:
                sampleGenerator
                        = new RootedPageRankSampler(sentences, seed, numNode, negative);
        }
        for (int i = 0; i < deltas.length; i++) { // backup the model for inplace update.
            deltas[i] = layers[i];
        }
        double sum_loss = 0.0;
        int loss_cnt = 0;
        while (sampleGenerator.hasNext()) {
            Tuple3<Integer, Integer, Integer> dataPoint = ((RootedPageRankSampler) sampleGenerator).next();
            if(dataPoint == null) break;

            int src = dataPoint._1(), dst = dataPoint._2(), label = dataPoint._3();
            if (!index.containsKey(src) || !index.containsKey(dst)) System.out.println("Error");

            int srcOffset = index.get(src) * dimension * 2 + dimension; // word embedding
            int dstOffset = index.get(dst) * dimension * 2; // context emdedding
            float wc = 0.0f;
            for (int c = 0; c < dimension; c++) wc += layers[c + srcOffset] * layers[c + dstOffset];

            // calculate loss
            float prob = FastSigmoid.sigmoid(wc);
            if (label == 0)
                sum_loss -= FastSigmoid.log(1 - prob);
            else
                sum_loss -= FastSigmoid.log(prob);
            loss_cnt++;

            // update model
            float g = -(label - prob) * alpha;
            // word, context
            float tmp;
            for (int c = 0; c < dimension; c++) {
                tmp = layers[c + srcOffset];
                layers[c + srcOffset] -= g * layers[c + dstOffset];
                layers[c + dstOffset] -= g * tmp;
            }
        }
        // calculate the deltas for pushing to parameter server.
        for(int i = 0; i < layers.length; i++){
            deltas[i] = layers[i] - deltas[i];
        }
        return new Tuple2<>(sum_loss, loss_cnt);
    }

}
