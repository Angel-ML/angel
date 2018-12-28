package com.tencent.angel.spark.ml.tree.gbdt.histogram;

import java.io.Serializable;
import java.util.Arrays;

public class Histogram implements Serializable {
    private int numBin;
    private int numClass;
    private boolean fullHessian;
    private double[] gradients;
    private double[] hessians;

    public Histogram(int numBin, int numClass, boolean fullHessian) {
        this.numBin = numBin;
        this.numClass = numClass;
        this.fullHessian = fullHessian;
        if (numClass == 2) {
            this.gradients = new double[numBin];
            this.hessians = new double[numBin];
        } else if (!fullHessian) {
            this.gradients = new double[numBin * numClass];
            this.hessians = new double[numBin * numClass];
        } else {
            this.gradients = new double[numBin * numClass];
            this.hessians = new double[numBin * ((numClass * (numClass + 1)) >> 1)];
        }
    }

    public void accumulate(int index, GradPair gradPair) {
        if (numClass == 2) {
            BinaryGradPair binary = (BinaryGradPair) gradPair;
            gradients[index] += binary.getGrad();
            hessians[index] += binary.getHess();
        } else if (!fullHessian) {
            MultiGradPair multi = (MultiGradPair) gradPair;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                gradients[offset + i] += grad[i];
                hessians[offset + i] += hess[i];
            }
        } else {
            MultiGradPair multi = (MultiGradPair) gradPair;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int gradOffset = index * numClass;
            int hessOffset = index * ((numClass * (numClass + 1)) >> 1);
            for (int i = 0; i < grad.length; i++)
                gradients[gradOffset + i] += grad[i];
            for (int i = 0; i < hess.length; i++)
                hessians[hessOffset + i] += hess[i];
        }
    }

    public Histogram plus(Histogram other) {
        Histogram res = new Histogram(numBin, numClass, fullHessian);
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                res.gradients[i] = this.gradients[i] + other.gradients[i];
                res.hessians[i] = this.hessians[i] + other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                res.gradients[i] = this.gradients[i] + other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                res.hessians[i] = this.hessians[i] + other.hessians[i];
        }
        return res;
    }

    public Histogram subtract(Histogram other) {
        Histogram res = new Histogram(numBin, numClass, fullHessian);
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                res.gradients[i] = this.gradients[i] - other.gradients[i];
                res.hessians[i] = this.hessians[i] - other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                res.gradients[i] = this.gradients[i] - other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                res.hessians[i] = this.hessians[i] - other.hessians[i];
        }
        return res;
    }

    public void plusBy(Histogram other) {
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                this.gradients[i] += other.gradients[i];
                this.hessians[i] += other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                this.gradients[i] += other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                this.hessians[i] += other.hessians[i];
        }
    }

    public void subtractBy(Histogram other) {
        if (numClass == 2 || !fullHessian) {
            for (int i = 0; i < this.gradients.length; i++) {
                this.gradients[i] -= other.gradients[i];
                this.hessians[i] -= other.hessians[i];
            }
        } else {
            for (int i = 0; i < this.gradients.length; i++)
                this.gradients[i] -= other.gradients[i];
            for (int i = 0; i < this.hessians.length; i++)
                this.hessians[i] -= other.hessians[i];
        }
    }

    public GradPair sum() {
        return sum(0, numBin);
    }

    public GradPair sum(int start, int end) {
        if (numClass == 2) {
            double sumGrad = 0.0;
            double sumHess = 0.0;
            for (int i = start; i < end; i++) {
                sumGrad += gradients[i];
                sumHess += hessians[i];
            }
            return new BinaryGradPair(sumGrad, sumHess);
        } else if (!fullHessian) {
            double[] sumGrad = new double[numClass];
            double[] sumHess = new double[numClass];
            for (int i = start * numClass; i < end * numClass; i += numClass) {
                for (int j = 0; j < numClass; j++) {
                    sumGrad[j] += gradients[i + j];
                    sumHess[j] += hessians[i + j];
                }
            }
            return new MultiGradPair(sumGrad, sumHess);
        } else {
            double[] sumGrad = new double[numClass];
            double[] sumHess = new double[(numClass * (numClass + 1)) >> 1];
            for (int i = start; i < end; i++) {
                int gradOffset = i * sumGrad.length;
                for (int j = 0; j < sumGrad.length; j++)
                    sumGrad[j] += gradients[gradOffset + j];
                int hessOffset = i * sumHess.length;
                for (int j = 0; j < sumHess.length; j++)
                    sumHess[j] += hessians[hessOffset + j];
            }
            return new MultiGradPair(sumGrad, sumHess);
        }
    }

    public int getNumBin() {
        return numBin;
    }

    public GradPair get(int index) {
        if (numClass == 2) {
            return new BinaryGradPair(gradients[index], hessians[index]);
        } else {
            double[] grad = Arrays.copyOfRange(gradients,
                    index * numClass, (index + 1) * numClass);
            int size = fullHessian ? ((numClass * (numClass + 1)) >> 1) : numClass;
            double[] hess = Arrays.copyOfRange(hessians,
                    index * size, (index + 1) * size);
            return new MultiGradPair(grad, hess);
        }
    }

    public void plusTo(GradPair gp, int index) {
        if (numClass == 2) {
            ((BinaryGradPair) gp).plusBy(gradients[index], hessians[index]);
        } else if (!fullHessian) {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                grad[i] += gradients[offset + i];
                hess[i] += hessians[offset + i];
            }
        } else {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int gradOffset = index * grad.length;
            int hessOffset = index * hess.length;
            for (int i = 0; i < grad.length; i++)
                grad[i] += gradients[gradOffset + i];
            for (int i = 0; i < hess.length; i++)
                hess[i] += hessians[hessOffset + i];
        }
    }

    public void subtractTo(GradPair gp, int index) {
        if (numClass == 2) {
            ((BinaryGradPair) gp).subtractBy(gradients[index], hessians[index]);
        } else if (!fullHessian) {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int offset = index * numClass;
            for (int i = 0; i < numClass; i++) {
                grad[i] -= gradients[offset + i];
                hess[i] -= hessians[offset + i];
            }
        } else {
            MultiGradPair multi = (MultiGradPair) gp;
            double[] grad = multi.getGrad();
            double[] hess = multi.getHess();
            int gradOffset = index * grad.length;
            int hessOffset = index * hess.length;
            for (int i = 0; i < grad.length; i++)
                grad[i] -= gradients[gradOffset + i];
            for (int i = 0; i < hess.length; i++)
                hess[i] -= hessians[hessOffset + i];
        }
    }
}
