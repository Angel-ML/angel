package com.tencent.angel.spark.ml.embedding;

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntOpenHashSet;
import scala.Tuple2;

import java.util.Arrays;
import java.util.Random;

public class CBowModel {

  private int window;
  private int negative;
  private float alpha;
  private int numNode;
  private int dimension;


  public CBowModel(int window, int negative, float alpha, int numNode, int dimension) {
    this.window = window;
    this.negative = negative;
    this.alpha = alpha;
    this.numNode = numNode;
    this.dimension = dimension;
  }

  public int[] indicesForCbow(int[][] sentences, long seed) {
    Random winRand = new Random(seed);
    Random negRand = new Random(seed + 1);

    IntOpenHashSet indices = new IntOpenHashSet();
    for (int s = 0; s < sentences.length; s++) {
      int len = sentences[s].length;
      for (int p = 0; p < len; p++) {
        int word = sentences[s][p];
        indices.add(word);

        int b = winRand.nextInt(window);

        int cw = 0;

        for (int a = b; a < window * 2 + 1 - b; a++) {
          if (a != window) {
            int c = p - window + a;
            if (c < 0) continue;
            if (c > len) continue;
            cw ++;
          }
        }

        if (cw > 0) {
          int target;
          for (int d = 0; d < negative; d++) {
            target = negRand.nextInt(numNode);
            if (target != word)
              indices.add(target);
          }
        }
      }
    }
    return indices.toIntArray();
  }

  public Tuple2<Double, Integer> cbow(int[][] sentences, long seed, float[] layers, Int2IntOpenHashMap index, float[] deltas) {
    System.arraycopy(layers, 0, deltas, 0, layers.length);

    Random winRand = new Random(seed);
    Random negRand = new Random(seed + 1);

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
//              target = table[rand.nextInt(table_size - 1) + 1];
              target = negRand.nextInt(numNode);
              if (target == word) continue;
              label = 0;
            }

            int l2 = index.get(target) * dimension * 2 + dimension;
            float f = 0f;
            for (int c = 0; c < dimension; c++) f += neu1[c] * layers[c + l2];

            float prob = FastSigmoid.sigmoid(f);
            if (d == 0) sum_loss -= FastSigmoid.log(prob);
            else sum_loss -= FastSigmoid.log(1 - prob);
            loss_cnt ++;

//            if (d == 0)
//              sum_loss += -FastSigmoid.sigmoid(f);
//            else
//              sum_loss += -FastSigmoid.sigmoid(-f);

            // Using the sigmoid value from the pre-computed table
            float g = (label - FastSigmoid.sigmoid(f)) * alpha;
            // accumulate for the hidden layer
            for (int c = 0; c < dimension; c++) neu1e[c] += g * layers[c + l2];
            // update output layer
            for (int c = 0; c < dimension; c++) layers[l2 + c] += g * neu1[c];
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
              for (c = 0; c < dimension; c++) layers[c + offset] += neu1e[c];
            }
        }
      }
    }


    for (int a = 0; a < layers.length; a++) deltas[a] = layers[a] - deltas[a];
    return new Tuple2<>(sum_loss, loss_cnt);
  }

}
