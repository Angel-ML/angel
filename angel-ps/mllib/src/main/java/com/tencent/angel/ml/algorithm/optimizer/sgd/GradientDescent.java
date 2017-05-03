package com.tencent.angel.ml.algorithm.optimizer.sgd;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.math.TAbstractVector;
import com.tencent.angel.ml.math.vector.TDoubleVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import com.tencent.angel.ml.model.PSModel;
import com.tencent.angel.psagent.matrix.MatrixClient;
import com.tencent.angel.worker.storage.Storage;
import com.tencent.angel.worker.task.TaskContext;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class GradientDescent {

  private static final Log LOG = LogFactory.getLog(GradientDescent.class);

  //TODO duplicated method
  public static TDoubleVector runMiniBatchL2SGD(MatrixClient matrixClient, TAbstractVector[] xList,
                                                double[] yList, TDoubleVector w, double lr, int
                                                    batchSize, Loss loss) throws Exception {
    if (yList.length < batchSize || xList.length < batchSize) {
      LOG.error("Sample number less than batch size.");
      return null;
    }

    long startT = System.currentTimeMillis();
    SparseDoubleVector grad = new SparseDoubleVector(w.getDimension());

    for (int i = 0; i < batchSize; i++) {
      double gradScalar = loss.grad(w.dot(xList[i]), yList[i]);
      grad.plusBy(xList[i], -1.0 * gradScalar);
    }

    grad.timesBy(1 / (double) batchSize);

    if (loss.isL2Reg()) {
      for (int index : grad.getIndices()) {
        if (grad.get(index) > 10e-7) {
          grad.set(index, grad.get(index) + w.get(index) * ((L2Loss) loss).getRegParam());
        }
      }
    }

    // update weight
    w.plusBy(grad, -1.0 * lr);

    matrixClient.increment(w.getRowId(), grad.timesBy(-1.0 * lr));

    LOG.info("run one mini batch sgd cost: " + (System.currentTimeMillis() - startT) + "ms");

    return w;
  }

  //TODO duplicated method
  public static TDoubleVector runMiniBatchSGD(MatrixClient matrixClient,
                                              Storage<LabeledData> featureStorage, TDoubleVector
                                                  w, double lr, int batchSize, Loss loss)
      throws Exception {

    double squareLoss = 0.0;
    int innerbatchSize = 100000;
    TAbstractVector[] xList = new TAbstractVector[innerbatchSize];
    double[] yList = new double[innerbatchSize];

    LOG.info("total train sample in storage " + featureStorage.getTotalElemNum());

    int index = 0;
    int totalSampleNumber = 0;
    LabeledData data = null;
    while (totalSampleNumber++ < batchSize) {
      data = featureStorage.read();
      if (data == null) {
        featureStorage.resetReadIndex();
        data = featureStorage.read();
      }

      xList[index] = data.getX();
      yList[index] = data.getY();
      index++;

      if (index == innerbatchSize) {
        long startTs = System.currentTimeMillis();
        SparseDoubleVector grad = new SparseDoubleVector(w.getDimension());
        for (int i = 0; i < innerbatchSize; i++) {
          double dot = w.dot(xList[i]);
          double gradScalar = loss.grad(dot, yList[i]);
          grad.plusBy(xList[i], -1.0 * gradScalar);
          squareLoss += loss.loss(dot, yList[i]);
        }

        grad.timesBy(1 / (double) innerbatchSize);

        w.plusBy(grad, -1.0 * lr);
        matrixClient.increment(w.getRowId(), grad.timesBy(-1.0 * lr));

        LOG.info("train use time is " + (System.currentTimeMillis() - startTs)
            + ", train use sample number is " + totalSampleNumber + ", loss is " + (squareLoss)
            / totalSampleNumber + ", w sparsity is " + w.sparsity());

        index = 0;
      }
    }

    return w;
  }

  //TODO duplicated method
  public static TDoubleVector runMiniL2BatchSGD(MatrixClient matrixClient,
                                                Storage<LabeledData> featureStorage,
                                                TDoubleVector w, double lr, int trainNum,
                                                int innerbatchSize, Loss loss, double regParam)
      throws Exception {

    double squareLoss = 0.0;
    TAbstractVector[] xList = new TAbstractVector[innerbatchSize];
    double[] yList = new double[innerbatchSize];

    LOG.info("total train sample in storage " + featureStorage.getTotalElemNum());

    int index = 0;
    int totalSampleNumber = 0;
    double regVal = 0.0;

    trainNum = ((int) (trainNum / innerbatchSize)) * innerbatchSize;
    LabeledData data = null;
    while (totalSampleNumber++ < trainNum) {
      data = featureStorage.read();
      if (data == null) {
        featureStorage.resetReadIndex();
        data = featureStorage.read();
      }

      xList[index] = data.getX();
      yList[index] = data.getY();
      index++;

      if (index == innerbatchSize) {
        long startTs = System.currentTimeMillis();
        SparseDoubleVector grad = new SparseDoubleVector(w.getDimension());
        for (int i = 0; i < innerbatchSize; i++) {
          double dot = w.dot(xList[i]);
          double gradScalar = loss.grad(dot, yList[i]);
          grad.plusBy(xList[i], -1.0 * gradScalar);
          squareLoss += loss.loss(dot, yList[i]);
        }

        if (regParam != 0.0) {
          w.timesBy(1.0 - lr * regParam);
          double norm = Math.sqrt(w.dot(w));
          regVal = 0.5 * regParam * norm * norm;
        }

        grad.timesBy(1 / (double) innerbatchSize);

        w.plusBy(grad, -1.0 * lr);
        matrixClient.increment(w.getRowId(), grad.timesBy(-1.0 * lr));

        LOG.info("train use time is " + (System.currentTimeMillis() - startTs)
            + ", train use sample number is " + totalSampleNumber + ", loss is " + (squareLoss)
            / totalSampleNumber + regVal + ", w sparsity is " + w.sparsity());

        index = 0;
      }
    }

    return w;
  }

  /**
   * Apply mini-batch gradient descent to a weight vector w
   *
   * @param matClient    matrix client of the matrix
   * @param trainData    storage of train dataset
   * @param w            weight vector of each feature
   * @param lr           learning rate
   * @param batchNum     number of batch, we seperate samples into #batchNum mini-batches, and
   *                     update
   *                     the weight vector with a mini-batch of samples iteratively
   * @param sampPerBatch number of samples per mini-bath
   * @param loss         type of loss, we use log loss for logistic regression
   * @return updated weight vector
   * @throws Exception
   */
  public static TDoubleVector runMiniL2BatchSGD(MatrixClient matClient,
                                                Storage<LabeledData> trainData, TDoubleVector w,
                                                double lr,
                                                int batchNum, int sampPerBatch, Loss loss) throws
      Exception {

    long startTs = System.currentTimeMillis();
    double totalLoss = 0.0;

    trainData.resetReadIndex();

    for (int batch = 0; batch < batchNum; batch++) {

      SparseDoubleVector grad = new SparseDoubleVector(w.getDimension());
      long curBatchStartTs = System.currentTimeMillis();
      LabeledData data;
      double batchLoss = 0.0;

      for (int i = 0; i < sampPerBatch; i++) {
        data = trainData.read();
        TAbstractVector x = data.getX();
        double pre = w.dot(x);
        double y = data.getY();
        double gradScalar = loss.grad(pre, y);

        grad.plusBy(x, -1.0 * gradScalar);

        batchLoss += loss.loss(pre, y);
      }

      totalLoss += batchLoss;
      grad.timesBy(1 / (double) sampPerBatch);
      w.plusBy(grad, -1.0 * lr);

      matClient.increment(w.getRowId(), grad.timesBy(-1.0 * lr));

      LOG.info(String.format(
          "Batch[%d] cost time: %d ms, sample number: %d, batch loss: %f", batch, System
              .currentTimeMillis() - curBatchStartTs, sampPerBatch, batchLoss));
    }

    LOG.info(String.format(
        "Train cost time: %d ms, sample number: %d, batch number: %d, average batch loss: %f",
        System.currentTimeMillis() - startTs, batchNum * sampPerBatch, batchNum, totalLoss /
            batchNum));

    return w;
  }

  public static void runMiniL2BatchSGD(Storage<LabeledData> trainData,
                                                TDoubleVector w, PSModel psModel, double lr, int
                                                    batchNum, int
                                                    sampPerBatch, Loss
                                                    loss) throws
      Exception {

    long startTs = System.currentTimeMillis();
    double totalLoss = 0.0;

    trainData.resetReadIndex();

    for (int batch = 0; batch < batchNum; batch++) {
      SparseDoubleVector grad = new SparseDoubleVector(w.getDimension());
      grad.setRowId(0);

      long curBatchStartTs = System.currentTimeMillis();
      LabeledData data;
      double batchLoss = 0.0;

      for (int i = 0; i < sampPerBatch; i++) {
        data = trainData.read();
        TAbstractVector x = data.getX();
        double pre = w.dot(x);
        double y = data.getY();
        double gradScalar = loss.grad(pre, y);

        grad.plusBy(x, -1.0 * gradScalar);

        batchLoss += loss.loss(pre, y);
      }

      totalLoss += batchLoss;
      grad.timesBy(1 / (double) sampPerBatch);
      w.plusBy(grad, -1.0 * lr);

      psModel.increment(grad.timesBy(-1.0 * lr));
      LOG.info(String.format(
          "Batch[%d] cost time: %d ms, sample number: %d, batch loss: %f", batch, System
              .currentTimeMillis() - curBatchStartTs, sampPerBatch, batchLoss));
    }

    LOG.info(String.format(
        "Train cost time: %d ms, sample number: %d, batch number: %d, average batch loss: %f",
        System.currentTimeMillis() - startTs, batchNum * sampPerBatch, batchNum, totalLoss /
            batchNum));
  }

  public static TDoubleVector runMiniL1BatchSGD(MatrixClient matrixClient, TaskContext context,
                                                Storage<LabeledData> featureStorage,
                                                TDoubleVector w, double lr, int iterNum,
                                                int batchSize, Loss loss) throws Exception {

    long startTs = System.currentTimeMillis();
    double totalLoss = 0.0;

    int totalSampleNumber = 0;
    int curBatchNum = 0;
    int matrixId = matrixClient.getMatrixId();

    // conduct truncated gradient to the weight
    if (context.getMatrixClock(matrixId) > 0) {
      // LOG.info(String.format("Weight vector at clock[%d]: %s, sparsity[%f]",
      // context.getClock(), Arrays.toString(w.getValues()).substring(0, 1000), w.sparsity()));

      int curTrunc = truncateCount;

      TDoubleVector truncUpdate = truncGradient(w, lr * loss.getRegParam(), loss.getRegParam());

      LOG.info(String.format("Truncate %d gradients at clock[%d], sparsity[%f]", truncateCount
          - curTrunc, context.getMatrixClock(matrixId), w.sparsity()));

      // the leader worker (the first worker) update the global weight on the PS
      if (context.getTaskIndex() == 0) {
        matrixClient.increment(0, truncUpdate);
      }
    }

    while (curBatchNum++ < iterNum) {

      int index = 0;
      double curBatchLoss = 0.0;
      SparseDoubleVector grad = new SparseDoubleVector(w.getDimension());
      long curBatchStartTs = System.currentTimeMillis();

      LabeledData data = null;
      while (index < batchSize) {
        data = featureStorage.read();
        if (data == null) {
          featureStorage.resetReadIndex();
          continue;
        }

        index++;
        totalSampleNumber++;

        TAbstractVector x = data.getX();
        double pre = w.dot(x);
        double y = data.getY();
        double gradScalar = loss.grad(pre, y);
        grad.plusBy(x, -1.0 * gradScalar);
        curBatchLoss += loss.loss(pre, y);

        if (index == batchSize) {
          if (loss.isL1Reg()) {
            curBatchLoss += loss.getReg(w);
          }
          totalLoss += curBatchLoss;
          grad.timesBy(1 / (double) batchSize);
          w.plusBy(grad, -1.0 * lr);
          matrixClient.increment(w.getRowId(), grad.timesBy(-1.0 * lr));
          LOG.info(String.format("Batch[%d] cost time: %d ms, sample number: %d, batch loss: %f",
              curBatchNum, System.currentTimeMillis() - curBatchStartTs, index, curBatchLoss));
        }
      }
    }

    LOG.info(String.format("Train cost time: %d ms, sample number: %d, batch number: %d, loss: %f",
        System.currentTimeMillis() - startTs, totalSampleNumber, curBatchNum, totalLoss
            / curBatchNum));
    return w;
  }


  public static int truncateCount = 0;

  /*
   * T(v, alpha, theta) = max(0, v-alpha), if v in [0,theta] min(0, v-alpha), if v in [-theta,0) v,
   * otherwise
   *
   * this function returns the update to the vector
   */
  public static TDoubleVector truncGradient(TDoubleVector vec, double alpha, double theta) {
    TDoubleVector update = new SparseDoubleVector(vec.getDimension());
    for (int dim = 0; dim < vec.getDimension(); dim++) {
      double value = vec.get(dim);
      if (value >= 0 && value <= theta) {
        double newValue = value - alpha > 0 ? value - alpha : 0;
        vec.set(dim, newValue);
        update.set(dim, newValue - value);
        truncateCount++;
        // if (truncateCount % 500000 == 0) {
        // LOG.info(String.format("Truncate gradient dim[%d] from [%f] to [%f], with theta[%f]
        // alpha[%f]",
        // dim, value, newValue, theta, alpha));
        // }
      } else if (value < 0 && value >= -theta) {
        double newValue = value - alpha < 0 ? value - alpha : 0;
        vec.set(dim, newValue);
        update.set(dim, newValue - value);
        truncateCount++;
        // if (truncateCount % 500000 == 0) {
        // LOG.info(String.format("Truncate gradient dim[%d] from [%f] to [%f], with theta[%f]
        // alpha[%f]",
        // dim, value, newValue, theta, alpha));
        // }
      }
    }
    return update;
  }

}