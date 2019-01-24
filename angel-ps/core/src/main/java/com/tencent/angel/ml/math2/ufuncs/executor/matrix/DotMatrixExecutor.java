/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/Apache-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */


package com.tencent.angel.ml.math2.ufuncs.executor.matrix;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.MFactory;
import com.tencent.angel.ml.math2.matrix.*;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.vector.*;
import com.github.fommil.netlib.BLAS;
import it.unimi.dsi.fastutil.ints.*;
import com.tencent.angel.ml.math2.MatrixExecutors;
import it.unimi.dsi.fastutil.objects.ObjectIterator;
import com.tencent.angel.ml.math2.utils.ArrayCopy;
import com.tencent.angel.ml.matrix.RowType;
import java.util.concurrent.RecursiveAction;


public class DotMatrixExecutor {
    private static BLAS blas = BLAS.getInstance();

    public static Vector apply(Matrix mat, boolean trans, Vector v) {
        if (mat instanceof BlasDoubleMatrix && v instanceof IntDoubleVector) {
            return apply((BlasDoubleMatrix) mat, trans, (IntDoubleVector) v);
        } else if (mat instanceof BlasDoubleMatrix && v instanceof IntFloatVector) {
            return apply((BlasDoubleMatrix) mat, trans, (IntFloatVector) v);
        } else if (mat instanceof BlasDoubleMatrix && v instanceof IntLongVector) {
            return apply((BlasDoubleMatrix) mat, trans, (IntLongVector) v);
        } else if (mat instanceof BlasDoubleMatrix && v instanceof IntIntVector) {
            return apply((BlasDoubleMatrix) mat, trans, (IntIntVector) v);
        } else if (mat instanceof BlasDoubleMatrix && v instanceof IntDummyVector) {
            return apply((BlasDoubleMatrix) mat, trans, (IntDummyVector) v);
        } else if (mat instanceof BlasFloatMatrix && v instanceof IntFloatVector) {
            return apply((BlasFloatMatrix) mat, trans, (IntFloatVector) v);
        } else if (mat instanceof BlasFloatMatrix && v instanceof IntLongVector) {
            return apply((BlasFloatMatrix) mat, trans, (IntLongVector) v);
        } else if (mat instanceof BlasFloatMatrix && v instanceof IntIntVector) {
            return apply((BlasFloatMatrix) mat, trans, (IntIntVector) v);
        } else if (mat instanceof BlasFloatMatrix && v instanceof IntDummyVector) {
            return apply((BlasFloatMatrix) mat, trans, (IntDummyVector) v);
        } else if (mat instanceof RowBasedMatrix) {
            if (trans) {
                return ((RowBasedMatrix) mat).transDot(v);
            } else {
                return mat.dot(v);
            }
        } else {
            throw new AngelException("the operation is not supported!");
        }
    }

    private static Vector apply(BlasDoubleMatrix mat, boolean trans, IntDummyVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        double [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new double [n];
        } else {
            assert n == v.getDim();
            resArr = new double [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        double [ ] data = mat.getData();

        if (trans) {
            for (int j=0; j< c; j++) {
                int [ ] idxs = v.getIndices();
                for (int i: idxs) {
                    resArr[j] += data[i*c+j];
                }
            }
        } else {
            for (int i=0; i< r; i++) {
                int [ ] idxs = v.getIndices();
                for (int j:idxs) {
                    resArr[i] += data[i*c+j];
                }
            }
        }

        IntDoubleDenseVectorStorage storage = new IntDoubleDenseVectorStorage(resArr);
        return new IntDoubleVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasDoubleMatrix mat, boolean trans, IntDoubleVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        double  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new double [n];
        } else {
            assert n == v.getDim();
            resArr = new double [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        double [ ] data = mat.getData();
        if (v.isDense()) {
            double [] tempArray = v.getStorage().getValues();
            if (trans) {
                blas.dgemv("N", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            } else {
                blas.dgemv("T", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2DoubleMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getDoubleValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2DoubleMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2DoubleMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getDoubleValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    double [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    double  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntDoubleDenseVectorStorage storage = new IntDoubleDenseVectorStorage(resArr);
        return new IntDoubleVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasDoubleMatrix mat, boolean trans, IntFloatVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        double  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new double [n];
        } else {
            assert n == v.getDim();
            resArr = new double [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        double [ ] data = mat.getData();
        if (v.isDense()) {
            double [] tempArray = ArrayCopy.copy(v.getStorage().getValues(), new double[v.getDim()]);
            if (trans) {
                blas.dgemv("N", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            } else {
                blas.dgemv("T", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2FloatMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getFloatValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2FloatMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getFloatValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    float [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    float  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntDoubleDenseVectorStorage storage = new IntDoubleDenseVectorStorage(resArr);
        return new IntDoubleVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasDoubleMatrix mat, boolean trans, IntLongVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        double  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new double [n];
        } else {
            assert n == v.getDim();
            resArr = new double [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        double [ ] data = mat.getData();
        if (v.isDense()) {
            double [] tempArray = ArrayCopy.copy(v.getStorage().getValues(), new double[v.getDim()]);
            if (trans) {
                blas.dgemv("N", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            } else {
                blas.dgemv("T", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2LongMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getLongValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2LongMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getLongValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    long [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    long  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntDoubleDenseVectorStorage storage = new IntDoubleDenseVectorStorage(resArr);
        return new IntDoubleVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasDoubleMatrix mat, boolean trans, IntIntVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        double  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new double [n];
        } else {
            assert n == v.getDim();
            resArr = new double [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        double [ ] data = mat.getData();
        if (v.isDense()) {
            double [] tempArray = ArrayCopy.copy(v.getStorage().getValues(), new double[v.getDim()]);
            if (trans) {
                blas.dgemv("N", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            } else {
                blas.dgemv("T", c, r, 1.0, data, c, tempArray, 1, 0.0, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2IntMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getIntValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2IntMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getIntValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    int [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    int  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntDoubleDenseVectorStorage storage = new IntDoubleDenseVectorStorage(resArr);
        return new IntDoubleVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasFloatMatrix mat, boolean trans, IntDummyVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        float [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new float [n];
        } else {
            assert n == v.getDim();
            resArr = new float [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        float [ ] data = mat.getData();

        if (trans) {
            for (int j=0; j< c; j++) {
                int [ ] idxs = v.getIndices();
                for (int i: idxs) {
                    resArr[j] += data[i*c+j];
                }
            }
        } else {
            for (int i=0; i< r; i++) {
                int [ ] idxs = v.getIndices();
                for (int j:idxs) {
                    resArr[i] += data[i*c+j];
                }
            }
        }

        IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(resArr);
        return new IntFloatVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasFloatMatrix mat, boolean trans, IntFloatVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        float  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new float [n];
        } else {
            assert n == v.getDim();
            resArr = new float [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        float [ ] data = mat.getData();
        if (v.isDense()) {
            float [] tempArray = v.getStorage().getValues();
            if (trans) {
                blas.sgemv("N", c, r, 1.0f, data, c, tempArray, 1, 0.0f, resArr, 1);
            } else {
                blas.sgemv("T", c, r, 1.0f, data, c, tempArray, 1, 0.0f, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2FloatMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getFloatValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2FloatMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2FloatMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getFloatValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    float [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    float  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(resArr);
        return new IntFloatVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasFloatMatrix mat, boolean trans, IntLongVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        float  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new float [n];
        } else {
            assert n == v.getDim();
            resArr = new float [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        float [ ] data = mat.getData();
        if (v.isDense()) {
            float [] tempArray = ArrayCopy.copy(v.getStorage().getValues(), new float[v.getDim()]);
            if (trans) {
                blas.sgemv("N", c, r, 1.0f, data, c, tempArray, 1, 0.0f, resArr, 1);
            } else {
                blas.sgemv("T", c, r, 1.0f, data, c, tempArray, 1, 0.0f, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2LongMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getLongValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2LongMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2LongMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getLongValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    long [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    long  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(resArr);
        return new IntFloatVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }

    private static Vector apply(BlasFloatMatrix mat, boolean trans, IntIntVector v) {
        int m = mat.getNumRows(), n = mat.getNumCols();
        float  [ ] resArr;
        if (trans) {
            assert m == v.getDim();
            resArr = new float [n];
        } else {
            assert n == v.getDim();
            resArr = new float [m];
        }

        int r = mat.getNumRows(), c = mat.getNumCols();
        float [ ] data = mat.getData();
        if (v.isDense()) {
            float [] tempArray = ArrayCopy.copy(v.getStorage().getValues(), new float[v.getDim()]);
            if (trans) {
                blas.sgemv("N", c, r, 1.0f, data, c, tempArray, 1, 0.0f, resArr, 1);
            } else {
                blas.sgemv("T", c, r, 1.0f, data, c, tempArray, 1, 0.0f, resArr, 1);
            }
        } else if (v.isSparse()) {
            if (trans) {
                for (int j=0; j < c; j++) {
                    ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2IntMap.Entry entry = iter.next();
                        int i = entry.getIntKey();
                        resArr[j] += data[i*c+j] * entry.getIntValue();
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    ObjectIterator<Int2IntMap.Entry> iter = v.getStorage().entryIterator();
                    while (iter.hasNext()) {
                        Int2IntMap.Entry entry = iter.next();
                        int j = entry.getIntKey();
                        resArr[i] += data[i*c+j] * entry.getIntValue();
                    }
                }
            }
        } else { // sorted
            if (trans) {
                for (int j=0; j< r; j++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    int [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[j] += data[idxs[k]*c+j] * vals[k];
                    }
                }
            } else {
                for (int i=0; i< r; i++) {
                    int [ ] idxs = v.getStorage().getIndices();
                    int  [ ] vals = v.getStorage().getValues();
                    for (int k=0; k< idxs.length; k++) {
                        resArr[i] += data[i*c+idxs[k]] * vals[k];
                    }
                }
            }
        }

        IntFloatDenseVectorStorage storage = new IntFloatDenseVectorStorage(resArr);
        return new IntFloatVector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);
    }


  public static Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2,
      Boolean parallel) {
    if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof BlasDoubleMatrix) {
      if (parallel) {
        return applyParallel((BlasDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2);
      } else {
        return apply((BlasDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2);
      }
    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof BlasFloatMatrix) {
      if (parallel) {
        return applyParallel((BlasFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2);
      } else {
        return apply((BlasFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2);
      }
    } else if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof RBIntDoubleMatrix) {
      return apply((BlasDoubleMatrix) mat1, trans1, (RBIntDoubleMatrix) mat2, trans2);
    } else if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof RBLongDoubleMatrix) {
      return apply((BlasDoubleMatrix) mat1, trans1, (RBLongDoubleMatrix) mat2, trans2);
    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof RBIntFloatMatrix) {
      return apply((BlasFloatMatrix) mat1, trans1, (RBIntFloatMatrix) mat2, trans2);
    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof RBLongFloatMatrix) {
      return apply((BlasFloatMatrix) mat1, trans1, (RBLongFloatMatrix) mat2, trans2);
    } else if (mat1 instanceof RBIntDoubleMatrix && mat2 instanceof BlasDoubleMatrix) {
      return apply((RBIntDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2);
    } else if (mat1 instanceof RBIntFloatMatrix && mat2 instanceof BlasFloatMatrix) {
      return apply((RBIntFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2);
    } else if (mat1 instanceof RowBasedMatrix && mat2 instanceof RowBasedMatrix) {
      if (!trans1 && trans2) {
        int outputRow = mat1.getNumRows();
        int outputCol = mat2.getNumRows();
        RowType type1 = mat1.getRow(0).getStorage().getType();
        RowType type2 = mat2.getRow(0).getStorage().getType();

        if (type1.isDouble() && type2.isDouble()) {
          BlasDoubleMatrix res = MFactory.denseDoubleMatrix(outputRow, outputCol);
          for (int i = 0; i < outputCol; i++) {
            Vector row = mat2.getRow(i);
            Vector col = mat1.dot(row);
            res.setCol(i, col);
          }
          return res;
        } else if (type1.isFloat() && type2.isFloat()) {
          BlasFloatMatrix res = MFactory.denseFloatMatrix(outputRow, outputCol);
          for (int i = 0; i < outputCol; i++) {
            Vector row = mat2.getRow(i);
            Vector col = mat1.dot(row);
            res.setCol(i, col);
          }
          return res;
        } else {
          throw new AngelException("the operation is not supported!");
        }
      } else {
        throw new AngelException("the operation is not supported!");
      }
    } else {
      throw new AngelException("the operation is not supported!");
    }
  }

    private static Matrix apply(BlasDoubleMatrix mat1, boolean trans1, BlasDoubleMatrix mat2, boolean trans2) {
        double alpha = 1.0, beta = 0.0;
        double [ ] resBlas;

        int m = mat1.getNumRows(), n = mat1.getNumCols();
        int p = mat2.getNumRows(), q = mat2.getNumCols();

        if (trans1 && trans2) { // M1^T * M2^T
            assert m == q;
            resBlas = new double[n*p];

            blas.dgemm("T", "T", p, n, m, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, p);

            return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, p, resBlas);
        } else if (!trans1 && trans2) { // M1 * M2^T
            assert n == q;
            resBlas = new double[m*p];

            blas.dgemm("T", "N", p, m, n, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, p);

            return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, p, resBlas);
        } else if (trans1 && !trans2) { // M1^T * M2
            assert m == p;
            resBlas = new double[n*q];

            blas.dgemm("N", "T", q, n, m, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, q);

            return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, q, resBlas);
        } else { // M1 * M2
            assert n == p;
            resBlas = new double[m*q];

            blas.dgemm("N", "N", q, m, n, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, q);

            return new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, q, resBlas);
        }
    }

    private static Matrix apply(BlasFloatMatrix mat1, boolean trans1, BlasFloatMatrix mat2, boolean trans2) {
        float alpha = 1.0f, beta = 0.0f;
        float [ ] resBlas;

        int m = mat1.getNumRows(), n = mat1.getNumCols();
        int p = mat2.getNumRows(), q = mat2.getNumCols();

        if (trans1 && trans2) { // M1^T * M2^T
            assert m == q;
            resBlas = new float[n*p];

            blas.sgemm("T", "T", p, n, m, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, p);

            return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), n, p, resBlas);
        } else if (!trans1 && trans2) { // M1 * M2^T
            assert n == q;
            resBlas = new float[m*p];

            blas.sgemm("T", "N", p, m, n, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, p);

            return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, p, resBlas);
        } else if (trans1 && !trans2) { // M1^T * M2
            assert m == p;
            resBlas = new float[n*q];

            blas.sgemm("N", "T", q, n, m, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, q);

            return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), n, q, resBlas);
        } else { // M1 * M2
            assert n == p;
            resBlas = new float[m*q];

            blas.sgemm("N", "N", q, m, n, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, q);

            return new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, q, resBlas);
        }
    }

  private static Matrix applyParallel(BlasDoubleMatrix mat1, boolean trans1, BlasDoubleMatrix mat2,
      boolean trans2) {

    int m = mat1.getNumRows(), n = mat1.getNumCols();
    int p = mat2.getNumRows(), q = mat2.getNumCols();

    double [] resBlas;
    BlasDoubleMatrix retMat;
    BlasDoubleMatrix transMat1;
    MatrixExecutors executors = MatrixExecutors.getInstance();

    if (trans1) {
      if (trans2) {
        assert m == q;
        resBlas = new double [n * p];
        retMat = new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, p, resBlas);
      } else {
        assert m == p;
        resBlas = new double [n * q];
        retMat = new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, q, resBlas);
      }

      // Transform mat1, generate a new matrix
      transMat1 = new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), n, m, transform(mat1));
    } else {
      if (trans2) {
        assert n == q;
        resBlas = new double [m * p];
        retMat = new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, p, resBlas);
      } else {
        assert n == p;
        resBlas = new double [m * q];
        retMat = new BlasDoubleMatrix(mat1.getMatrixId(), mat1.getClock(), m, q, resBlas);
      }

      transMat1 = mat1;
    }

    // Split the row indices of mat1Trans
    int subM = Math.max(1, transMat1.getNumRows() / executors.getParallel());
    int[] leftRowOffIndices = splitRowIds(transMat1.getNumRows(), subM);

    // Parallel execute use fork-join
    DotForkJoinOp op = new DotForkJoinOp(transMat1, mat2, retMat, leftRowOffIndices, 0,
        leftRowOffIndices.length, subM, trans2);
    executors.execute(op);
    op.join();

    return retMat;
  }

  private static void apply(BlasDoubleMatrix leftMatrix, BlasDoubleMatrix rightMatrix,
      BlasDoubleMatrix resultMatrix, int[] leftRowOffIndices,
      int startPos, int endPos, int subM, boolean trans2, double alpha, double beta) {
    // Get the sub-matrix of left matrix, split by row
    double [] leftData = leftMatrix.getData();
    int splitRowNum = subM;
    if (endPos == leftRowOffIndices.length) {
      splitRowNum = leftMatrix.getNumRows() - subM * (leftRowOffIndices.length - 1);
    }
    double [] splitLeftData = new double [splitRowNum * leftMatrix.getNumCols()];
    System.arraycopy(leftData, leftRowOffIndices[startPos] * leftMatrix.getNumCols(),
        splitLeftData, 0, splitLeftData.length);

    double [] resultData = resultMatrix.getData();

    if (trans2) {
      double [] splitResult = new double [splitRowNum * rightMatrix.getNumRows()];
      blas.dgemm(
          "T", "N", rightMatrix.getNumRows(), splitRowNum, leftMatrix.getNumCols(),
          alpha, rightMatrix.getData(), rightMatrix.getNumCols(), splitLeftData,
          leftMatrix.getNumCols(), beta, splitResult, rightMatrix.getNumRows());
      System.arraycopy(splitResult, 0, resultData,
          leftRowOffIndices[startPos] * rightMatrix.getNumRows(), splitResult.length);
    } else {
      double [] splitResult = new double [splitRowNum * rightMatrix.getNumCols()];
      blas.dgemm(
          "N", "N", rightMatrix.getNumCols(), splitRowNum, leftMatrix.getNumCols(),
          alpha, rightMatrix.getData(), rightMatrix.getNumCols(), splitLeftData,
          leftMatrix.getNumCols(), beta, splitResult, rightMatrix.getNumCols());
      System.arraycopy(splitResult, 0, resultData,
          leftRowOffIndices[startPos] * rightMatrix.getNumCols(), splitResult.length);
    }
  }

  private static Matrix applyParallel(BlasFloatMatrix mat1, boolean trans1, BlasFloatMatrix mat2,
      boolean trans2) {

    int m = mat1.getNumRows(), n = mat1.getNumCols();
    int p = mat2.getNumRows(), q = mat2.getNumCols();

    float [] resBlas;
    BlasFloatMatrix retMat;
    BlasFloatMatrix transMat1;
    MatrixExecutors executors = MatrixExecutors.getInstance();

    if (trans1) {
      if (trans2) {
        assert m == q;
        resBlas = new float [n * p];
        retMat = new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), n, p, resBlas);
      } else {
        assert m == p;
        resBlas = new float [n * q];
        retMat = new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), n, q, resBlas);
      }

      // Transform mat1, generate a new matrix
      transMat1 = new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), n, m, transform(mat1));
    } else {
      if (trans2) {
        assert n == q;
        resBlas = new float [m * p];
        retMat = new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, p, resBlas);
      } else {
        assert n == p;
        resBlas = new float [m * q];
        retMat = new BlasFloatMatrix(mat1.getMatrixId(), mat1.getClock(), m, q, resBlas);
      }

      transMat1 = mat1;
    }

    // Split the row indices of mat1Trans
    int subM = Math.max(1, transMat1.getNumRows() / executors.getParallel());
    int[] leftRowOffIndices = splitRowIds(transMat1.getNumRows(), subM);

    // Parallel execute use fork-join
    DotForkJoinOp op = new DotForkJoinOp(transMat1, mat2, retMat, leftRowOffIndices, 0,
        leftRowOffIndices.length, subM, trans2);
    executors.execute(op);
    op.join();

    return retMat;
  }

  private static void apply(BlasFloatMatrix leftMatrix, BlasFloatMatrix rightMatrix,
      BlasFloatMatrix resultMatrix, int[] leftRowOffIndices,
      int startPos, int endPos, int subM, boolean trans2, float alpha, float beta) {
    // Get the sub-matrix of left matrix, split by row
    float [] leftData = leftMatrix.getData();
    int splitRowNum = subM;
    if (endPos == leftRowOffIndices.length) {
      splitRowNum = leftMatrix.getNumRows() - subM * (leftRowOffIndices.length - 1);
    }
    float [] splitLeftData = new float [splitRowNum * leftMatrix.getNumCols()];
    System.arraycopy(leftData, leftRowOffIndices[startPos] * leftMatrix.getNumCols(),
        splitLeftData, 0, splitLeftData.length);

    float [] resultData = resultMatrix.getData();

    if (trans2) {
      float [] splitResult = new float [splitRowNum * rightMatrix.getNumRows()];
      blas.sgemm(
          "T", "N", rightMatrix.getNumRows(), splitRowNum, leftMatrix.getNumCols(),
          alpha, rightMatrix.getData(), rightMatrix.getNumCols(), splitLeftData,
          leftMatrix.getNumCols(), beta, splitResult, rightMatrix.getNumRows());
      System.arraycopy(splitResult, 0, resultData,
          leftRowOffIndices[startPos] * rightMatrix.getNumRows(), splitResult.length);
    } else {
      float [] splitResult = new float [splitRowNum * rightMatrix.getNumCols()];
      blas.sgemm(
          "N", "N", rightMatrix.getNumCols(), splitRowNum, leftMatrix.getNumCols(),
          alpha, rightMatrix.getData(), rightMatrix.getNumCols(), splitLeftData,
          leftMatrix.getNumCols(), beta, splitResult, rightMatrix.getNumCols());
      System.arraycopy(splitResult, 0, resultData,
          leftRowOffIndices[startPos] * rightMatrix.getNumCols(), splitResult.length);
    }
  }


  /**
   * A simple parallel matrix dot operator
   */
  public static class DotForkJoinOp extends RecursiveAction {

    private final BlasMatrix leftMatrix;
    private final BlasMatrix rightMatrix;
    private final BlasMatrix resultMatrix;
    private final int[] leftRowOffIndices;
    private final int startPos;
    private final int endPos;
    private final int subM;
    private final boolean trans2;
    float alpha = 1.0f, beta = 0.0f;

    public DotForkJoinOp(BlasMatrix leftMatrix, BlasMatrix rightMatrix,
        BlasMatrix resultMatrix, int[] leftRowOffIndices,
        int startPos, int endPos, int subM, boolean trans2) {
      this.leftMatrix = leftMatrix;
      this.rightMatrix = rightMatrix;
      this.resultMatrix = resultMatrix;
      this.leftRowOffIndices = leftRowOffIndices;
      this.startPos = startPos;
      this.endPos = endPos;
      this.subM = subM;
      this.trans2 = trans2;
    }

    @Override
    protected void compute() {
      if (endPos <= startPos) {
        return;
      } else if (endPos - startPos == 1) {
        if (leftMatrix instanceof BlasDoubleMatrix && rightMatrix instanceof BlasDoubleMatrix) {
          apply((BlasDoubleMatrix) leftMatrix, (BlasDoubleMatrix) rightMatrix,
              (BlasDoubleMatrix) resultMatrix,
              leftRowOffIndices, startPos, endPos, subM, trans2, alpha, beta);
        } else if (leftMatrix instanceof BlasFloatMatrix
            && rightMatrix instanceof BlasFloatMatrix) {
          apply((BlasFloatMatrix) leftMatrix, (BlasFloatMatrix) rightMatrix,
              (BlasFloatMatrix) resultMatrix,
              leftRowOffIndices, startPos, endPos, subM, trans2, alpha, beta);
        } else {
          throw new AngelException("not support");
        }
      } else {
        int middle = (startPos + endPos) / 2;
        DotForkJoinOp leftOp = new DotForkJoinOp(leftMatrix, rightMatrix, resultMatrix,
            leftRowOffIndices, startPos, middle, subM, trans2);
        DotForkJoinOp rightOp = new DotForkJoinOp(leftMatrix, rightMatrix, resultMatrix,
            leftRowOffIndices, middle, endPos, subM, trans2);
        invokeAll(leftOp, rightOp);
      }
    }
  }

  private static double [] transform(BlasDoubleMatrix matrix) {
    double [] data = matrix.getData();
    double [] ret = new double [data.length];
    int m = matrix.getNumRows();
    int n = matrix.getNumCols();
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        ret[j * m + i] = data[i * n + j];
      }
    }
    return ret;
  }

  private static float [] transform(BlasFloatMatrix matrix) {
    float [] data = matrix.getData();
    float [] ret = new float [data.length];
    int m = matrix.getNumRows();
    int n = matrix.getNumCols();
    for (int i = 0; i < m; i++) {
      for (int j = 0; j < n; j++) {
        ret[j * m + i] = data[i * n + j];
      }
    }
    return ret;
  }


  private static int[] splitRowIds(int rowNum, int subM) {
    int splitNums;
    if (rowNum % subM == 0) {
      splitNums = rowNum / subM;
    } else {
      splitNums = rowNum / subM + 1;
    }

    int[] ret = new int[splitNums];
    for (int i = 0; i < splitNums; i++) {
      ret[i] = subM * i;
    }
    return ret;
  }

  private static Matrix apply(BlasDoubleMatrix mat1, boolean trans1, RBIntDoubleMatrix mat2,
      boolean trans2) {
    if (trans1 && !trans2) {
      int outputRows = mat1.getNumCols();
      IntDoubleVector[] rows = new IntDoubleVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector col = mat1.getCol(i);
        rows[i] = (IntDoubleVector) mat2.transDot(col);
      }
      return MFactory.rbIntDoubleMatrix(rows);
    } else if (!trans1 && !trans2) {
      int outputRows = mat1.getNumRows();
      IntDoubleVector[] rows = new IntDoubleVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector row = mat1.getRow(i);
        rows[i] = (IntDoubleVector) mat2.transDot(row);
      }
      return MFactory.rbIntDoubleMatrix(rows);
    } else {
      throw new AngelException("the operation is not supported!");
    }
  }

  private static Matrix apply(BlasDoubleMatrix mat1, boolean trans1, RBLongDoubleMatrix mat2,
      boolean trans2) {
    if (trans1 && !trans2) {
      int outputRows = mat1.getNumCols();
      LongDoubleVector[] rows = new LongDoubleVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector col = mat1.getCol(i);
        rows[i] = (LongDoubleVector) mat2.transDot(col);
      }
      return MFactory.rbLongDoubleMatrix(rows);
    } else if (!trans1 && !trans2) {
      int outputRows = mat1.getNumRows();
      LongDoubleVector[] rows = new LongDoubleVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector row = mat1.getRow(i);
        rows[i] = (LongDoubleVector) mat2.transDot(row);
      }
      return MFactory.rbLongDoubleMatrix(rows);
    } else {
      throw new AngelException("the operation is not supported!");
    }
  }

  private static Matrix apply(BlasFloatMatrix mat1, boolean trans1, RBIntFloatMatrix mat2,
      boolean trans2) {
    if (trans1 && !trans2) {
      int outputRows = mat1.getNumCols();
      IntFloatVector[] rows = new IntFloatVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector col = mat1.getCol(i);
        rows[i] = (IntFloatVector) mat2.transDot(col);
      }
      return MFactory.rbIntFloatMatrix(rows);
    } else if (!trans1 && !trans2) {
      int outputRows = mat1.getNumRows();
      IntFloatVector[] rows = new IntFloatVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector row = mat1.getRow(i);
        rows[i] = (IntFloatVector) mat2.transDot(row);
      }
      return MFactory.rbIntFloatMatrix(rows);
    } else {
      throw new AngelException("the operation is not supported!");
    }
  }

  private static Matrix apply(BlasFloatMatrix mat1, boolean trans1, RBLongFloatMatrix mat2,
      boolean trans2) {
    if (trans1 && !trans2) {
      int outputRows = mat1.getNumCols();
      LongFloatVector[] rows = new LongFloatVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector col = mat1.getCol(i);
        rows[i] = (LongFloatVector) mat2.transDot(col);
      }
      return MFactory.rbLongFloatMatrix(rows);
    } else if (!trans1 && !trans2) {
      int outputRows = mat1.getNumRows();
      LongFloatVector[] rows = new LongFloatVector[outputRows];
      for (int i = 0; i < outputRows; i++) {
        Vector row = mat1.getRow(i);
        rows[i] = (LongFloatVector) mat2.transDot(row);
      }
      return MFactory.rbLongFloatMatrix(rows);
    } else {
      throw new AngelException("the operation is not supported!");
    }
  }



  private static Matrix apply(RBIntDoubleMatrix mat1, boolean trans1, BlasDoubleMatrix mat2,
      boolean trans2) {
    if (trans1 && trans2) {
      // mat1 trans true, mat trans true
      int outputRows = (int) mat1.getDim();
      int outputCols = mat2.getNumRows();
      BlasDoubleMatrix res = MFactory.denseDoubleMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Row = mat2.getRow(i);
        Vector outCol = mat1.transDot(mat2Row);
        res.setCol(i, outCol);
      }
      return res;
    } else if (trans1 && !trans2) {
      // mat1 trans true, mat trans false
      int outputRows = (int) mat1.getDim();
      int outputCols = mat2.getNumCols();
      BlasDoubleMatrix res = MFactory.denseDoubleMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Col = mat2.getCol(i);
        Vector outCol = mat1.transDot(mat2Col);
        res.setCol(i, outCol);
      }
      return res;
    } else if (!trans1 && trans2) {
      // mat1 trans false, mat trans true, important
      int outputRows = mat1.getNumRows();
      int outputCols = mat2.getNumRows();
      BlasDoubleMatrix res = MFactory.denseDoubleMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Row = mat2.getRow(i);
        Vector outCol = mat1.dot(mat2Row);
        res.setCol(i, outCol);
      }
      return res;
    } else { // !trans1 && !trans2
      // mat1 trans false, mat trans false
      int outputRows = mat1.getNumRows();
      int outputCols = mat2.getNumCols();
      BlasDoubleMatrix res = MFactory.denseDoubleMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Col = mat2.getCol(i);
        Vector outCol = mat1.dot(mat2Col);
        res.setCol(i, outCol);
      }

      return res;
    }
  }

  private static Matrix apply(RBIntFloatMatrix mat1, boolean trans1, BlasFloatMatrix mat2,
      boolean trans2) {
    if (trans1 && trans2) {
      // mat1 trans true, mat trans true
      int outputRows = (int) mat1.getDim();
      int outputCols = mat2.getNumRows();
      BlasFloatMatrix res = MFactory.denseFloatMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Row = mat2.getRow(i);
        Vector outCol = mat1.transDot(mat2Row);
        res.setCol(i, outCol);
      }
      return res;
    } else if (trans1 && !trans2) {
      // mat1 trans true, mat trans false
      int outputRows = (int) mat1.getDim();
      int outputCols = mat2.getNumCols();
      BlasFloatMatrix res = MFactory.denseFloatMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Col = mat2.getCol(i);
        Vector outCol = mat1.transDot(mat2Col);
        res.setCol(i, outCol);
      }
      return res;
    } else if (!trans1 && trans2) {
      // mat1 trans false, mat trans true, important
      int outputRows = mat1.getNumRows();
      int outputCols = mat2.getNumRows();
      BlasFloatMatrix res = MFactory.denseFloatMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Row = mat2.getRow(i);
        Vector outCol = mat1.dot(mat2Row);
        res.setCol(i, outCol);
      }
      return res;
    } else { // !trans1 && !trans2
      // mat1 trans false, mat trans false
      int outputRows = mat1.getNumRows();
      int outputCols = mat2.getNumCols();
      BlasFloatMatrix res = MFactory.denseFloatMatrix(outputRows, outputCols);
      for (int i = 0; i < outputCols; i++) {
        Vector mat2Col = mat2.getCol(i);
        Vector outCol = mat1.dot(mat2Col);
        res.setCol(i, outCol);
      }

      return res;
    }
  }


    public static double apply(Matrix mat, Vector v1){
        if (mat instanceof BlasMatrix){
            assert ((BlasMatrix) mat).getNumRows() == ((BlasMatrix) mat).getNumCols();
            return DotMatrixExecutor.apply(mat, true, v1).dot(v1);
        } else {
            return mat.dot(v1).dot(v1);
        }
    }

    public static double apply(Matrix mat, Vector v1, Vector v2){
        if (mat instanceof BlasMatrix) {
            if (!v2.isDense()) {
                return DotMatrixExecutor.apply(mat, false, v2).dot(v1);
            } else {
                return DotMatrixExecutor.apply(mat, true, v1).dot(v2);
            }
        } else {
            return mat.dot(v2).dot(v1);
        }
    }

    public static Matrix apply(Matrix mat, double alpha, Vector v1, Vector v2){
        assert mat instanceof BlasMatrix;
        if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDoubleVector && v2 instanceof IntDoubleVector) {
            return apply((BlasDoubleMatrix)mat, alpha, (IntDoubleVector) v1, (IntDoubleVector) v2);
        }else if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDummyVector && v2 instanceof IntDoubleVector) {
            return apply((BlasDoubleMatrix)mat, alpha, (IntDummyVector) v1, (IntDoubleVector) v2);
        } else if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDoubleVector && v2 instanceof IntDummyVector) {
            return apply((BlasDoubleMatrix)mat, alpha, (IntDoubleVector) v1, (IntDummyVector) v2);
        } else if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDummyVector && v2 instanceof IntDummyVector) {
            return apply((BlasDoubleMatrix)mat, alpha, (IntDummyVector) v1, (IntDummyVector) v2);
        } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntFloatVector && v2 instanceof IntFloatVector){
            return apply((BlasFloatMatrix)mat, (float)alpha, (IntFloatVector) v1, (IntFloatVector) v2);
        } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntDummyVector && v2 instanceof IntFloatVector){
            return apply((BlasFloatMatrix)mat, (float)alpha, (IntDummyVector) v1, (IntFloatVector) v2);
        } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntFloatVector && v2 instanceof IntDummyVector){
            return apply((BlasFloatMatrix)mat, (float)alpha, (IntFloatVector) v1, (IntDummyVector) v2);
        } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntDummyVector && v2 instanceof IntDummyVector){
            return apply((BlasFloatMatrix)mat, (float)alpha, (IntDummyVector) v1, (IntDummyVector) v2);
        } else {
            throw new AngelException("The operation is not supported!");
        }
    }

    private static Matrix apply(BlasDoubleMatrix mat, double alpha, IntDoubleVector v1, IntDoubleVector v2) {
        double [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        if (v1.isDense() && v2.isDense()) {
            double [ ] v1Value = v1.getStorage().getValues();
            double [ ] v2Value = v2.getStorage().getValues();
            // dger(int m, int n, double alpha, double [ ] x, int incx, double [ ] y, int incy, double [ ] a, int lda)
            blas.dger(n, m, alpha, v2Value, 1, v1Value, 1, data, n);
            mat.setData(data);
        } else if (v1.isDense() && v2.isSparse()) {
            double [ ] v1Values = v1.getStorage().getValues();
            for(int i=0; i< m; i++) { // row
                ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
                while(iter.hasNext()) {
                    Int2DoubleMap.Entry entry = iter.next();
                    int j = entry.getIntKey(); // col
                    data[i*n+j] += alpha * v1Values[i] * entry.getDoubleValue();
                }
            }
        } else if (v1.isDense() && v2.isSorted()) {
            double [ ] v1Values = v1.getStorage().getValues();
            int size = v2.size();
            for(int i=0; i< m; i++) { // row
                int [ ] v2Idxs = v2.getStorage().getIndices();
                double [ ] v2Values = v2.getStorage().getValues();
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * v1Values[i] * v2Values[k];
                }
            }
        } else if (v1.isSparse() && v2.isDense()) {
            double [ ] v2Values = v2.getStorage().getValues();
            ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
            while(iter.hasNext()) {
                Int2DoubleMap.Entry entry = iter.next();
                int i = entry.getIntKey(); // row
                for (int j=0; j< n; j++) {
                    data[i*n+j] += alpha * entry.getDoubleValue() * v2Values[j];
                }
            }
        } else if (v1.isSparse() && v2.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> outer = v1.getStorage().entryIterator();
            while(outer.hasNext()) {
                Int2DoubleMap.Entry entry1 = outer.next();
                int i = entry1.getIntKey(); // row
                ObjectIterator<Int2DoubleMap.Entry> inner = v2.getStorage().entryIterator();
                while(inner.hasNext()) {
                    Int2DoubleMap.Entry entry2 = inner.next();
                    int j = entry2.getIntKey(); // col
                    data[i*n+j] += alpha * entry1.getDoubleValue() * entry2.getDoubleValue();
                }
            }
        } else if (v1.isSparse() && v2.isSorted()) {
            int [ ] v2Idxs = v2.getStorage().getIndices();
            double [ ] v2Values = v2.getStorage().getValues();
            ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
            int size = v2.size();
            while(iter.hasNext()) {
                Int2DoubleMap.Entry entry = iter.next();
                int i = entry.getIntKey(); // row
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * entry.getDoubleValue() * v2Values[k];
                }
            }
        } else if (v1.isSorted() && v2.isDense()) {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            double [ ] v1Values = v1.getStorage().getValues();
            double [ ] v2Values = v2.getStorage().getValues();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                for (int j=0; j< n; j++) {
                    data[i*n+j] += alpha * v1Values[k] * v2Values[j];
                }
            }
        } else if (v1.isSorted() && v2.isSparse()) {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            double [ ] v1Values = v1.getStorage().getValues();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
                while (iter.hasNext()) {
                    Int2DoubleMap.Entry entry = iter.next();
                    int j = entry.getIntKey();
                    data[i*n+j] += alpha * v1Values[k] * entry.getDoubleValue();
                }
            }
        } else if (v1.isSorted() && v2.isSorted()) {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            double [ ] v1Values = v1.getStorage().getValues();
            int [ ] v2Idxs = v2.getStorage().getIndices();
            double [ ] v2Values = v2.getStorage().getValues();
            int size1 = v1.size();
            int size2 = v2.size();
            for (int k=0; k< size1; k++) {
                int i = v1Idxs[k];
                for (int t=0; t< size2; t++) {
                    int j = v2Idxs[t];
                    data[i*n+j] += alpha * v1Values[k] * v2Values[t];
                }
            }
        } else {
            throw new AngelException("The operation is not supported!");
        }

        return mat;
    }

    private static Matrix apply(BlasDoubleMatrix mat, double alpha, IntDummyVector v1, IntDoubleVector v2) {
        double [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        if (v2.isDense()) {
            int [ ] v1Idxs = v1.getIndices();
            double [ ] v2Values = v2.getStorage().getValues();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                for (int j=0; j< n; j++) {
                    data[i*n+j] += alpha * v2Values[j];
                }
            }
        } else if (v2.isSparse()) {
            int [ ] v1Idxs = v1.getIndices();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
                while (iter.hasNext()) {
                    Int2DoubleMap.Entry entry = iter.next();
                    int j = entry.getIntKey();
                    data[i*n+j] += alpha * entry.getDoubleValue();
                }
            }
        } else if (v2.isSorted()) {
            int [ ] v1Idxs = v1.getIndices();
            int [ ] v2Idxs = v2.getStorage().getIndices();
            double [ ] v2Values = v2.getStorage().getValues();
            int size1 = v1.size();
            int size2 = v2.size();
            for (int k=0; k< size1; k++) {
                int i = v1Idxs[k];
                for (int t=0; t< size2; t++) {
                    int j = v2Idxs[t];
                    data[i*n+j] += alpha * v2Values[t];
                }
            }
        } else {
            throw new AngelException("The operation is not supported!");
        }

        return mat;
    }

    private static Matrix apply(BlasDoubleMatrix mat, double alpha, IntDoubleVector v1, IntDummyVector v2) {
        double [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        if (v1.isDense()) {
            double [ ] v1Values = v1.getStorage().getValues();
            int size = v2.size();
            for(int i=0; i< m; i++) { // row
                int [ ] v2Idxs = v2.getIndices();
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * v1Values[i];
                }
            }
        } else if (v1.isSparse()) {
            int [ ] v2Idxs = v2.getIndices();
            ObjectIterator<Int2DoubleMap.Entry> iter = v1.getStorage().entryIterator();
            int size = v2.size();
            while(iter.hasNext()) {
                Int2DoubleMap.Entry entry = iter.next();
                int i = entry.getIntKey(); // row
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * entry.getDoubleValue();
                }
            }
        } else {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            double [ ] v1Values = v1.getStorage().getValues();
            int [ ] v2Idxs = v2.getIndices();
            int size1 = v1.size();
            int size2 = v2.size();
            for (int k=0; k< size1; k++) {
                int i = v1Idxs[k];
                for (int t=0; t< size2; t++) {
                    int j = v2Idxs[t];
                    data[i*n+j] += alpha * v1Values[k];
                }
            }
        }

        return mat;
    }

    private static Matrix apply(BlasDoubleMatrix mat, double alpha, IntDummyVector v1, IntDummyVector v2) {
        double [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        int [ ] v1Idxs = v1.getIndices();
        int [ ] v2Idxs = v2.getIndices();
        int size1 = v1.size();
        int size2 = v2.size();
        for (int k=0; k< size1; k++) {
            int i = v1Idxs[k];
            for (int t = 0; t < size2; t++) {
                int j = v2Idxs[t];
                data[i * n + j] += alpha;
            }
        }

        return mat;
    }

    private static Matrix apply(BlasFloatMatrix mat, float alpha, IntFloatVector v1, IntFloatVector v2) {
        float [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        if (v1.isDense() && v2.isDense()) {
            float [ ] v1Value = v1.getStorage().getValues();
            float [ ] v2Value = v2.getStorage().getValues();
            // sger(int m, int n, double alpha, double [ ] x, int incx, double [ ] y, int incy, double [ ] a, int lda)
            blas.sger(n, m, alpha, v2Value, 1, v1Value, 1, data, n);
            mat.setData(data);
        } else if (v1.isDense() && v2.isSparse()) {
            float [ ] v1Values = v1.getStorage().getValues();
            for(int i=0; i< m; i++) { // row
                ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
                while(iter.hasNext()) {
                    Int2FloatMap.Entry entry = iter.next();
                    int j = entry.getIntKey(); // col
                    data[i*n+j] += alpha * v1Values[i] * entry.getFloatValue();
                }
            }
        } else if (v1.isDense() && v2.isSorted()) {
            float [ ] v1Values = v1.getStorage().getValues();
            int size = v2.size();
            for(int i=0; i< m; i++) { // row
                int [ ] v2Idxs = v2.getStorage().getIndices();
                float [ ] v2Values = v2.getStorage().getValues();
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * v1Values[i] * v2Values[k];
                }
            }
        } else if (v1.isSparse() && v2.isDense()) {
            float [ ] v2Values = v2.getStorage().getValues();
            ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
            while(iter.hasNext()) {
                Int2FloatMap.Entry entry = iter.next();
                int i = entry.getIntKey(); // row
                for (int j=0; j< n; j++) {
                    data[i*n+j] += alpha * entry.getFloatValue() * v2Values[j];
                }
            }
        } else if (v1.isSparse() && v2.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> outer = v1.getStorage().entryIterator();
            while(outer.hasNext()) {
                Int2FloatMap.Entry entry1 = outer.next();
                int i = entry1.getIntKey(); // row
                ObjectIterator<Int2FloatMap.Entry> inner = v2.getStorage().entryIterator();
                while(inner.hasNext()) {
                    Int2FloatMap.Entry entry2 = inner.next();
                    int j = entry2.getIntKey(); // col
                    data[i*n+j] += alpha * entry1.getFloatValue() * entry2.getFloatValue();
                }
            }
        } else if (v1.isSparse() && v2.isSorted()) {
            int [ ] v2Idxs = v2.getStorage().getIndices();
            float [ ] v2Values = v2.getStorage().getValues();
            ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
            int size = v2.size();
            while(iter.hasNext()) {
                Int2FloatMap.Entry entry = iter.next();
                int i = entry.getIntKey(); // row
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * entry.getFloatValue() * v2Values[k];
                }
            }
        } else if (v1.isSorted() && v2.isDense()) {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            float [ ] v1Values = v1.getStorage().getValues();
            float [ ] v2Values = v2.getStorage().getValues();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                for (int j=0; j< n; j++) {
                    data[i*n+j] += alpha * v1Values[k] * v2Values[j];
                }
            }
        } else if (v1.isSorted() && v2.isSparse()) {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            float [ ] v1Values = v1.getStorage().getValues();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
                while (iter.hasNext()) {
                    Int2FloatMap.Entry entry = iter.next();
                    int j = entry.getIntKey();
                    data[i*n+j] += alpha * v1Values[k] * entry.getFloatValue();
                }
            }
        } else if (v1.isSorted() && v2.isSorted()) {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            float [ ] v1Values = v1.getStorage().getValues();
            int [ ] v2Idxs = v2.getStorage().getIndices();
            float [ ] v2Values = v2.getStorage().getValues();
            int size1 = v1.size();
            int size2 = v2.size();
            for (int k=0; k< size1; k++) {
                int i = v1Idxs[k];
                for (int t=0; t< size2; t++) {
                    int j = v2Idxs[t];
                    data[i*n+j] += alpha * v1Values[k] * v2Values[t];
                }
            }
        } else {
            throw new AngelException("The operation is not supported!");
        }

        return mat;
    }

    private static Matrix apply(BlasFloatMatrix mat, float alpha, IntDummyVector v1, IntFloatVector v2) {
        float [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        if (v2.isDense()) {
            int [ ] v1Idxs = v1.getIndices();
            float [ ] v2Values = v2.getStorage().getValues();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                for (int j=0; j< n; j++) {
                    data[i*n+j] += alpha * v2Values[j];
                }
            }
        } else if (v2.isSparse()) {
            int [ ] v1Idxs = v1.getIndices();
            int size = v1.size();
            for (int k=0; k< size; k++) {
                int i = v1Idxs[k];
                ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
                while (iter.hasNext()) {
                    Int2FloatMap.Entry entry = iter.next();
                    int j = entry.getIntKey();
                    data[i*n+j] += alpha * entry.getFloatValue();
                }
            }
        } else if (v2.isSorted()) {
            int [ ] v1Idxs = v1.getIndices();
            int [ ] v2Idxs = v2.getStorage().getIndices();
            float [ ] v2Values = v2.getStorage().getValues();
            int size1 = v1.size();
            int size2 = v2.size();
            for (int k=0; k< size1; k++) {
                int i = v1Idxs[k];
                for (int t=0; t< size2; t++) {
                    int j = v2Idxs[t];
                    data[i*n+j] += alpha * v2Values[t];
                }
            }
        } else {
            throw new AngelException("The operation is not supported!");
        }

        return mat;
    }

    private static Matrix apply(BlasFloatMatrix mat, float alpha, IntFloatVector v1, IntDummyVector v2) {
        float [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        if (v1.isDense()) {
            float [ ] v1Values = v1.getStorage().getValues();
            int size = v2.size();
            for(int i=0; i< m; i++) { // row
                int [ ] v2Idxs = v2.getIndices();
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * v1Values[i];
                }
            }
        } else if (v1.isSparse()) {
            int [ ] v2Idxs = v2.getIndices();
            ObjectIterator<Int2FloatMap.Entry> iter = v1.getStorage().entryIterator();
            int size = v2.size();
            while(iter.hasNext()) {
                Int2FloatMap.Entry entry = iter.next();
                int i = entry.getIntKey(); // row
                for (int k=0; k< size; k++) {
                    int j = v2Idxs[k]; // col
                    data[i*n+j] += alpha * entry.getFloatValue();
                }
            }
        } else {
            int [ ] v1Idxs = v1.getStorage().getIndices();
            float [ ] v1Values = v1.getStorage().getValues();
            int [ ] v2Idxs = v2.getIndices();
            int size1 = v1.size();
            int size2 = v2.size();
            for (int k=0; k< size1; k++) {
                int i = v1Idxs[k];
                for (int t=0; t< size2; t++) {
                    int j = v2Idxs[t];
                    data[i*n+j] += alpha * v1Values[k];
                }
            }
        }

        return mat;
    }

    private static Matrix apply(BlasFloatMatrix mat, float alpha, IntDummyVector v1, IntDummyVector v2) {
        float [ ] data = mat.getData();
        int m = mat.getNumRows(), n = mat.getNumCols();
        assert (m == v1.getDim() && n == v2.getDim());

        int [ ] v1Idxs = v1.getIndices();
        int [ ] v2Idxs = v2.getIndices();
        int size1 = v1.size();
        int size2 = v2.size();
        for (int k=0; k< size1; k++) {
            int i = v1Idxs[k];
            for (int t = 0; t < size2; t++) {
                int j = v2Idxs[t];
                data[i * n + j] += alpha;
            }
        }

        return mat;
    }

}
