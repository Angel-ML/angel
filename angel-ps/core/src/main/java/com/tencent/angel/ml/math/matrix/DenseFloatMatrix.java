/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.ml.math.matrix;

import com.tencent.angel.ml.math.vector.TFloatVector;
import com.tencent.angel.ml.math.TMatrix;
import com.tencent.angel.ml.math.TVector;
import com.tencent.angel.ml.math.vector.DenseDoubleVector;
import com.tencent.angel.ml.math.vector.DenseFloatVector;
import com.tencent.angel.ml.math.vector.SparseDoubleVector;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.Arrays;

/**
 * The Dense float matrix.
 */
public class DenseFloatMatrix extends TFloatMatrix {

    private static final Log LOG = LogFactory.getLog(DenseFloatMatrix.class);

    float[][] values;

    /**
     * Create a new empty matrix.
     *
     * @param row the row
     * @param col the col
     */
    public DenseFloatMatrix(int row, int col) {
        super(row, col);
        values = new float[row][];
    }

    /**
     * Create a new matrix with <code>values</code>.
     *
     * @param row    the row
     * @param col    the col
     * @param values the values
     */
    public DenseFloatMatrix(int row, int col, float[][] values) {
        super(row, col);
        this.values = values;
    }

    /**
     * inc the matrix
     *
     * @param rowId the row id
     * @param colId the col id
     * @param value the value
     */
    @Override
    public void inc(int rowId, int colId, float value) {
        if (values[rowId] == null)
            values[rowId] = new float[col];
        values[rowId][colId] += value;
    }

    public void inc(int rowId, int colId, double value) {
        if (values[rowId] == null)
            values[rowId] = new float[col];

        values[rowId][colId] += (float) value;
    }

    public void inc(int rowId, int colId, int value) {
        if (values[rowId] == null)
            values[rowId] = new float[col];

        values[rowId][colId] += value;
    }

    @Override
    public void plusBy(TMatrix other) {

    }


    /**
     * plus the matrix
     *
     * @param other the other
     */
    @Override
    public void plusBy(TVector other) {
        if (other instanceof DenseDoubleVector) {
            plusBy((DenseDoubleVector) other);
            return;
        }

        if (other instanceof DenseFloatVector) {
            plusBy((DenseFloatVector) other);
            return;
        }

        if (other instanceof SparseDoubleVector) {
            plusBy((SparseDoubleVector) other);
            return;
        }

        LOG.error(String.format("Unregisterd class type %s", other.getClass().getName()));
    }

    public void plusBy(DenseDoubleVector other) {
        int rowId = other.getRowId();
        int length = other.size();
        double[] deltas = other.getValues();

        if (this.values[rowId] == null)
            this.values[rowId] = new float[col];

        for (int i = 0; i < length; i++) {
            this.values[rowId][i] += deltas[i];
        }
    }

    public void plusBy(DenseFloatVector other) {
        int rowId = other.getRowId();
        int length = other.size();
        float[] deltas = other.getValues();

        if (this.values[rowId] == null)
            this.values[rowId] = new float[col];

        for (int i = 0; i < length; i++) {
            this.values[rowId][i] += deltas[i];
        }
    }

    public void plusBy(SparseDoubleVector other) {
        int rowId = other.getRowId();
        int[] indices = other.getIndices();

        if (this.values[rowId] == null)
            this.values[rowId] = new float[col];

        for (int i = 0; i < indices.length; i++) {
            double value = other.get(indices[i]);
            values[rowId][indices[i]] += value;
        }
    }

    /**
     * get the value of one element
     *
     * @param rowId the row id
     * @param colId the col id
     * @return
     */
    @Override
    public float get(int rowId, int colId) {
        if (this.values[rowId] == null)
            return 0;
        return values[rowId][colId];
    }

    /**
     * get the vector
     *
     * @param rowId the row id
     * @return
     */
    @Override
    public TFloatVector getTFloatVector(int rowId) {
        if (values[rowId] == null) {
            return null;
        } else {
            DenseFloatVector vector = new DenseFloatVector(col);
            for (int i = 0; i < col; i++) {
                vector.set(i, values[rowId][i]);
            }
            vector.setMatrixId(matrixId);
            vector.setRowId(rowId);
            return vector;
        }
    }

    /**
     * get the sparsity of matrix
     *
     * @return
     */
    @Override
    public double sparsity() {
        return (double) nonZeroNum() / (row * col);
    }

    /**
     * get the size of matrix
     *
     * @return
     */
    @Override
    public int size() {
        return row * col;
    }

    /**
     * clearn the matrix
     */
    @Override
    public void clear() {
        for (int i = 0; i < row; i++) {
            if (values[i] != null)
                Arrays.fill(values[i], 0.0f);
        }
    }

    /**
     * the count of nonzero element
     *
     * @return
     */
    @Override
    public int nonZeroNum() {
        int sum = 0;
        for (int i = 0; i < row; i++) {
            if (values[i] == null)
                continue;

            for (int j = 0; j < col; j++) {
                if (values[i][j] != 0) {
                    sum++;
                }
            }
        }
        return sum;
    }
}
