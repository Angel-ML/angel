package com.tencent.angel.ml.treemodels.gbdt.histogram;

import com.tencent.angel.ml.math.vector.DenseFloatVector;

/**
 * Histogram for GBDT nodes
 */
public class Histogram {
    private int numFeature;
    private int numSplit;
    private int numClass;

    private DenseFloatVector[] histograms;

    public Histogram(int numFeature, int numSplit, int numClass) {
        this.numFeature = numFeature;
        this.numSplit = numSplit;
        this.numClass = numClass;
        histograms = new DenseFloatVector[numFeature];
    }

    public void alloc() {
        int numHist = numClass == 2 ? 1 : numClass;
        int sizePerFeat = numHist * numSplit * 2;
        for (int i = 0; i < histograms.length; i++) {
            histograms[i] = new DenseFloatVector(sizePerFeat);
        }
    }

    public DenseFloatVector getHistogram(int index) {
        return histograms[index];
    }

    public void set(int index, DenseFloatVector hist) {
        this.histograms[index] = hist;
    }

    public Histogram plusBy(Histogram other) {
        for (int i = 0; i < histograms.length; i++) {
            int size = histograms[i].getDimension();
            float[] myValues = histograms[i].getValues();
            float[] otherValues = other.histograms[i].getValues();
            for (int j = 0; j < size; j++) {
                myValues[j] += otherValues[j];
            }
        }
        return this;
    }

    public Histogram subtract(Histogram other) {
        Histogram res = new Histogram(numFeature, numSplit, numClass);
        for (int i = 0; i < histograms.length; i++) {
            int size = histograms[i].getDimension();
            float[] resValue = new float[size];
            float[] aValue = this.histograms[i].getValues();
            float[] bValue = other.histograms[i].getValues();
            for (int j = 0; j < size; j++) {
                resValue[j] = aValue[j] - bValue[j];
            }
            res.histograms[i] = new DenseFloatVector(size, resValue);
        }
        return res;
    }

    public DenseFloatVector flatten() {
        int size = histograms.length * histograms[0].getDimension();
        float[] values = new float[size];
        int offset = 0;
        for (DenseFloatVector hist : histograms) {
            System.arraycopy(hist.getValues(), 0, values, offset, hist.getDimension());
            offset += hist.getDimension();
        }
        return new DenseFloatVector(size, values);
    }
}
