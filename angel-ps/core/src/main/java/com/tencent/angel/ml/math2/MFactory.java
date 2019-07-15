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


package com.tencent.angel.ml.math2;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.matrix.*;
import com.tencent.angel.ml.math2.vector.CompIntDoubleVector;
import com.tencent.angel.ml.math2.vector.CompIntFloatVector;
import com.tencent.angel.ml.math2.vector.CompIntIntVector;
import com.tencent.angel.ml.math2.vector.CompIntLongVector;
import com.tencent.angel.ml.math2.vector.CompLongDoubleVector;
import com.tencent.angel.ml.math2.vector.CompLongFloatVector;
import com.tencent.angel.ml.math2.vector.CompLongIntVector;
import com.tencent.angel.ml.math2.vector.CompLongLongVector;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import java.util.HashMap;

public class MFactory {

  public static RBCompIntDoubleMatrix rbCompIntDoubleMatrix(int matrixId, int clock,
      CompIntDoubleVector[] rows) {
    return new RBCompIntDoubleMatrix(matrixId, clock, rows);
  }

  public static RBCompIntDoubleMatrix rbCompIntDoubleMatrix(CompIntDoubleVector[] rows) {
    return new RBCompIntDoubleMatrix(rows);
  }

  public static RBCompIntDoubleMatrix rbCompIntDoubleMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim) {
    return new RBCompIntDoubleMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompIntDoubleMatrix rbCompIntDoubleMatrix(int numRows, int numCols, int subDim) {
    return new RBCompIntDoubleMatrix(numRows, numCols, subDim);
  }

  public static RBCompIntDoubleMatrix rbCompIntDoubleMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim, StorageType storageType) {
    CompIntDoubleVector[] rows = new CompIntDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntDoubleVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompIntDoubleMatrix(matrixId, clock, rows);
  }

  public static RBCompIntDoubleMatrix rbCompIntDoubleMatrix(int numRows, int numCols, int subDim,
      StorageType storageType) {
    CompIntDoubleVector[] rows = new CompIntDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntDoubleVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompIntDoubleMatrix(rows);
  }

  public static RBCompIntFloatMatrix rbCompIntFloatMatrix(int matrixId, int clock,
      CompIntFloatVector[] rows) {
    return new RBCompIntFloatMatrix(matrixId, clock, rows);
  }

  public static RBCompIntFloatMatrix rbCompIntFloatMatrix(CompIntFloatVector[] rows) {
    return new RBCompIntFloatMatrix(rows);
  }

  public static RBCompIntFloatMatrix rbCompIntFloatMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim) {
    return new RBCompIntFloatMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompIntFloatMatrix rbCompIntFloatMatrix(int numRows, int numCols, int subDim) {
    return new RBCompIntFloatMatrix(numRows, numCols, subDim);
  }

  public static RBCompIntFloatMatrix rbCompIntFloatMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim, StorageType storageType) {
    CompIntFloatVector[] rows = new CompIntFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntFloatVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompIntFloatMatrix(matrixId, clock, rows);
  }

  public static RBCompIntFloatMatrix rbCompIntFloatMatrix(int numRows, int numCols, int subDim,
      StorageType storageType) {
    CompIntFloatVector[] rows = new CompIntFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntFloatVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompIntFloatMatrix(rows);
  }

  public static RBCompIntLongMatrix rbCompIntLongMatrix(int matrixId, int clock,
      CompIntLongVector[] rows) {
    return new RBCompIntLongMatrix(matrixId, clock, rows);
  }

  public static RBCompIntLongMatrix rbCompIntLongMatrix(CompIntLongVector[] rows) {
    return new RBCompIntLongMatrix(rows);
  }

  public static RBCompIntLongMatrix rbCompIntLongMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim) {
    return new RBCompIntLongMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompIntLongMatrix rbCompIntLongMatrix(int numRows, int numCols, int subDim) {
    return new RBCompIntLongMatrix(numRows, numCols, subDim);
  }

  public static RBCompIntLongMatrix rbCompIntLongMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim, StorageType storageType) {
    CompIntLongVector[] rows = new CompIntLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntLongVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompIntLongMatrix(matrixId, clock, rows);
  }

  public static RBCompIntLongMatrix rbCompIntLongMatrix(int numRows, int numCols, int subDim,
      StorageType storageType) {
    CompIntLongVector[] rows = new CompIntLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntLongVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompIntLongMatrix(rows);
  }

  public static RBCompIntIntMatrix rbCompIntIntMatrix(int matrixId, int clock,
      CompIntIntVector[] rows) {
    return new RBCompIntIntMatrix(matrixId, clock, rows);
  }

  public static RBCompIntIntMatrix rbCompIntIntMatrix(CompIntIntVector[] rows) {
    return new RBCompIntIntMatrix(rows);
  }

  public static RBCompIntIntMatrix rbCompIntIntMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim) {
    return new RBCompIntIntMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompIntIntMatrix rbCompIntIntMatrix(int numRows, int numCols, int subDim) {
    return new RBCompIntIntMatrix(numRows, numCols, subDim);
  }

  public static RBCompIntIntMatrix rbCompIntIntMatrix(int matrixId, int clock, int numRows,
      int numCols, int subDim, StorageType storageType) {
    CompIntIntVector[] rows = new CompIntIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntIntVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompIntIntMatrix(matrixId, clock, rows);
  }

  public static RBCompIntIntMatrix rbCompIntIntMatrix(int numRows, int numCols, int subDim,
      StorageType storageType) {
    CompIntIntVector[] rows = new CompIntIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compIntIntVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompIntIntMatrix(rows);
  }

  public static RBCompLongDoubleMatrix rbCompLongDoubleMatrix(int matrixId, int clock,
      CompLongDoubleVector[] rows) {
    return new RBCompLongDoubleMatrix(matrixId, clock, rows);
  }

  public static RBCompLongDoubleMatrix rbCompLongDoubleMatrix(CompLongDoubleVector[] rows) {
    return new RBCompLongDoubleMatrix(rows);
  }

  public static RBCompLongDoubleMatrix rbCompLongDoubleMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim) {
    return new RBCompLongDoubleMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompLongDoubleMatrix rbCompLongDoubleMatrix(int numRows, long numCols,
      long subDim) {
    return new RBCompLongDoubleMatrix(numRows, numCols, subDim);
  }

  public static RBCompLongDoubleMatrix rbCompLongDoubleMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim, StorageType storageType) {
    CompLongDoubleVector[] rows = new CompLongDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongDoubleVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompLongDoubleMatrix(matrixId, clock, rows);
  }

  public static RBCompLongDoubleMatrix rbCompLongDoubleMatrix(int numRows, long numCols,
      long subDim, StorageType storageType) {
    CompLongDoubleVector[] rows = new CompLongDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongDoubleVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompLongDoubleMatrix(rows);
  }

  public static RBCompLongFloatMatrix rbCompLongFloatMatrix(int matrixId, int clock,
      CompLongFloatVector[] rows) {
    return new RBCompLongFloatMatrix(matrixId, clock, rows);
  }

  public static RBCompLongFloatMatrix rbCompLongFloatMatrix(CompLongFloatVector[] rows) {
    return new RBCompLongFloatMatrix(rows);
  }

  public static RBCompLongFloatMatrix rbCompLongFloatMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim) {
    return new RBCompLongFloatMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompLongFloatMatrix rbCompLongFloatMatrix(int numRows, long numCols,
      long subDim) {
    return new RBCompLongFloatMatrix(numRows, numCols, subDim);
  }

  public static RBCompLongFloatMatrix rbCompLongFloatMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim, StorageType storageType) {
    CompLongFloatVector[] rows = new CompLongFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongFloatVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompLongFloatMatrix(matrixId, clock, rows);
  }

  public static RBCompLongFloatMatrix rbCompLongFloatMatrix(int numRows, long numCols, long subDim,
      StorageType storageType) {
    CompLongFloatVector[] rows = new CompLongFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongFloatVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompLongFloatMatrix(rows);
  }

  public static RBCompLongLongMatrix rbCompLongLongMatrix(int matrixId, int clock,
      CompLongLongVector[] rows) {
    return new RBCompLongLongMatrix(matrixId, clock, rows);
  }

  public static RBCompLongLongMatrix rbCompLongLongMatrix(CompLongLongVector[] rows) {
    return new RBCompLongLongMatrix(rows);
  }

  public static RBCompLongLongMatrix rbCompLongLongMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim) {
    return new RBCompLongLongMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompLongLongMatrix rbCompLongLongMatrix(int numRows, long numCols, long subDim) {
    return new RBCompLongLongMatrix(numRows, numCols, subDim);
  }

  public static RBCompLongLongMatrix rbCompLongLongMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim, StorageType storageType) {
    CompLongLongVector[] rows = new CompLongLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongLongVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompLongLongMatrix(matrixId, clock, rows);
  }

  public static RBCompLongLongMatrix rbCompLongLongMatrix(int numRows, long numCols, long subDim,
      StorageType storageType) {
    CompLongLongVector[] rows = new CompLongLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongLongVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompLongLongMatrix(rows);
  }

  public static RBCompLongIntMatrix rbCompLongIntMatrix(int matrixId, int clock,
      CompLongIntVector[] rows) {
    return new RBCompLongIntMatrix(matrixId, clock, rows);
  }

  public static RBCompLongIntMatrix rbCompLongIntMatrix(CompLongIntVector[] rows) {
    return new RBCompLongIntMatrix(rows);
  }

  public static RBCompLongIntMatrix rbCompLongIntMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim) {
    return new RBCompLongIntMatrix(matrixId, clock, numRows, numCols, subDim);
  }

  public static RBCompLongIntMatrix rbCompLongIntMatrix(int numRows, long numCols, long subDim) {
    return new RBCompLongIntMatrix(numRows, numCols, subDim);
  }

  public static RBCompLongIntMatrix rbCompLongIntMatrix(int matrixId, int clock, int numRows,
      long numCols, long subDim, StorageType storageType) {
    CompLongIntVector[] rows = new CompLongIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongIntVector(matrixId, i, clock, numCols, subDim, storageType);
    }

    return new RBCompLongIntMatrix(matrixId, clock, rows);
  }

  public static RBCompLongIntMatrix rbCompLongIntMatrix(int numRows, long numCols, long subDim,
      StorageType storageType) {
    CompLongIntVector[] rows = new CompLongIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      rows[i] = VFactory.compLongIntVector(0, i, 0, numCols, subDim, storageType);
    }

    return new RBCompLongIntMatrix(rows);
  }

  //---------------------------------------------------

  public static RBIntDoubleMatrix rbIntDoubleMatrix(int matrixId, int clock,
      IntDoubleVector[] rows) {
    return new RBIntDoubleMatrix(matrixId, clock, rows);
  }

  public static RBIntDoubleMatrix rbIntDoubleMatrix(int matrixId, int clock, int numRows,
      int numCols) {
    return new RBIntDoubleMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBIntDoubleMatrix rbIntDoubleMatrix(int matrixId, int clock, int numRows,
      int numCols, StorageType storageType) {
    IntDoubleVector[] rows = new IntDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseDoubleVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseDoubleVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedDoubleVector(numCols);
          break;
      }
    }

    return new RBIntDoubleMatrix(matrixId, clock, rows);
  }

  public static RBIntDoubleMatrix rbIntDoubleMatrix(int numRows, int numCols,
      StorageType storageType) {
    IntDoubleVector[] rows = new IntDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseDoubleVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseDoubleVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedDoubleVector(numCols);
          break;
      }
    }

    return new RBIntDoubleMatrix(rows);
  }

  public static RBIntDoubleMatrix rbIntDoubleMatrix(IntDoubleVector[] rows) {
    return new RBIntDoubleMatrix(rows);
  }

  public static RBIntDoubleMatrix rbIntDoubleMatrix(int numRows, int numCols) {
    return new RBIntDoubleMatrix(numRows, numCols);
  }

  public static RBIntFloatMatrix rbIntFloatMatrix(int matrixId, int clock, IntFloatVector[] rows) {
    return new RBIntFloatMatrix(matrixId, clock, rows);
  }

  public static RBIntFloatMatrix rbIntFloatMatrix(int matrixId, int clock, int numRows,
      int numCols) {
    return new RBIntFloatMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBIntFloatMatrix rbIntFloatMatrix(int matrixId, int clock, int numRows, int numCols,
      StorageType storageType) {
    IntFloatVector[] rows = new IntFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseFloatVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseFloatVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedFloatVector(numCols);
          break;
      }
    }

    return new RBIntFloatMatrix(matrixId, clock, rows);
  }

  public static RBIntFloatMatrix rbIntFloatMatrix(int numRows, int numCols,
      StorageType storageType) {
    IntFloatVector[] rows = new IntFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseFloatVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseFloatVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedFloatVector(numCols);
          break;
      }
    }

    return new RBIntFloatMatrix(rows);
  }

  public static RBIntFloatMatrix rbIntFloatMatrix(IntFloatVector[] rows) {
    return new RBIntFloatMatrix(rows);
  }

  public static RBIntFloatMatrix rbIntFloatMatrix(int numRows, int numCols) {
    return new RBIntFloatMatrix(numRows, numCols);
  }

  public static RBIntLongMatrix rbIntLongMatrix(int matrixId, int clock, IntLongVector[] rows) {
    return new RBIntLongMatrix(matrixId, clock, rows);
  }

  public static RBIntLongMatrix rbIntLongMatrix(int matrixId, int clock, int numRows, int numCols) {
    return new RBIntLongMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBIntLongMatrix rbIntLongMatrix(int matrixId, int clock, int numRows, int numCols,
      StorageType storageType) {
    IntLongVector[] rows = new IntLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseLongVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseLongVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongVector(numCols);
          break;
      }
    }

    return new RBIntLongMatrix(matrixId, clock, rows);
  }

  public static RBIntLongMatrix rbIntLongMatrix(int numRows, int numCols, StorageType storageType) {
    IntLongVector[] rows = new IntLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseLongVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseLongVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongVector(numCols);
          break;
      }
    }

    return new RBIntLongMatrix(rows);
  }

  public static RBIntLongMatrix rbIntLongMatrix(IntLongVector[] rows) {
    return new RBIntLongMatrix(rows);
  }

  public static RBIntLongMatrix rbIntLongMatrix(int numRows, int numCols) {
    return new RBIntLongMatrix(numRows, numCols);
  }

  public static RBIntIntMatrix rbIntIntMatrix(int matrixId, int clock, IntIntVector[] rows) {
    return new RBIntIntMatrix(matrixId, clock, rows);
  }

  public static RBIntIntMatrix rbIntIntMatrix(int matrixId, int clock, int numRows, int numCols) {
    return new RBIntIntMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBIntIntMatrix rbIntIntMatrix(int matrixId, int clock, int numRows, int numCols,
      StorageType storageType) {
    IntIntVector[] rows = new IntIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseIntVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseIntVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedIntVector(numCols);
          break;
      }
    }

    return new RBIntIntMatrix(matrixId, clock, rows);
  }

  public static RBIntIntMatrix rbIntIntMatrix(int numRows, int numCols, StorageType storageType) {
    IntIntVector[] rows = new IntIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          rows[i] = VFactory.denseIntVector(numCols);
          break;
        case SPARSE:
          rows[i] = VFactory.sparseIntVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedIntVector(numCols);
          break;
      }
    }

    return new RBIntIntMatrix(rows);
  }

  public static RBIntIntMatrix rbIntIntMatrix(IntIntVector[] rows) {
    return new RBIntIntMatrix(rows);
  }

  public static RBIntIntMatrix rbIntIntMatrix(int numRows, int numCols) {
    return new RBIntIntMatrix(numRows, numCols);
  }

  public static RBLongDoubleMatrix rbLongDoubleMatrix(int matrixId, int clock,
      LongDoubleVector[] rows) {
    return new RBLongDoubleMatrix(matrixId, clock, rows);
  }

  public static RBLongDoubleMatrix rbLongDoubleMatrix(int matrixId, int clock, int numRows,
      long numCols) {
    return new RBLongDoubleMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBLongDoubleMatrix rbLongDoubleMatrix(int matrixId, int clock, int numRows,
      long numCols, StorageType storageType) {
    LongDoubleVector[] rows = new LongDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyDoubleVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyDoubleVector(numCols);
          break;
      }
    }

    return new RBLongDoubleMatrix(matrixId, clock, rows);
  }

  public static RBLongDoubleMatrix rbLongDoubleMatrix(int numRows, long numCols,
      StorageType storageType) {
    LongDoubleVector[] rows = new LongDoubleVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyDoubleVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyDoubleVector(numCols);
          break;
      }
    }

    return new RBLongDoubleMatrix(rows);
  }

  public static RBLongDoubleMatrix rbLongDoubleMatrix(LongDoubleVector[] rows) {
    return new RBLongDoubleMatrix(rows);
  }

  public static RBLongDoubleMatrix rbLongDoubleMatrix(int numRows, long numCols) {
    return new RBLongDoubleMatrix(numRows, numCols);
  }

  public static RBLongFloatMatrix rbLongFloatMatrix(int matrixId, int clock,
      LongFloatVector[] rows) {
    return new RBLongFloatMatrix(matrixId, clock, rows);
  }

  public static RBLongFloatMatrix rbLongFloatMatrix(int matrixId, int clock, int numRows,
      long numCols) {
    return new RBLongFloatMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBLongFloatMatrix rbLongFloatMatrix(int matrixId, int clock, int numRows,
      long numCols, StorageType storageType) {
    LongFloatVector[] rows = new LongFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyFloatVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyFloatVector(numCols);
          break;
      }
    }

    return new RBLongFloatMatrix(matrixId, clock, rows);
  }

  public static RBLongFloatMatrix rbLongFloatMatrix(int numRows, long numCols,
      StorageType storageType) {
    LongFloatVector[] rows = new LongFloatVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyFloatVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyFloatVector(numCols);
          break;
      }
    }

    return new RBLongFloatMatrix(rows);
  }

  public static RBLongFloatMatrix rbLongFloatMatrix(LongFloatVector[] rows) {
    return new RBLongFloatMatrix(rows);
  }

  public static RBLongFloatMatrix rbLongFloatMatrix(int numRows, long numCols) {
    return new RBLongFloatMatrix(numRows, numCols);
  }

  public static RBLongLongMatrix rbLongLongMatrix(int matrixId, int clock, LongLongVector[] rows) {
    return new RBLongLongMatrix(matrixId, clock, rows);
  }

  public static RBLongLongMatrix rbLongLongMatrix(int matrixId, int clock, int numRows,
      long numCols) {
    return new RBLongLongMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBLongLongMatrix rbLongLongMatrix(int matrixId, int clock, int numRows,
      long numCols, StorageType storageType) {
    LongLongVector[] rows = new LongLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyLongVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyLongVector(numCols);
          break;
      }
    }

    return new RBLongLongMatrix(matrixId, clock, rows);
  }

  public static RBLongLongMatrix rbLongLongMatrix(int numRows, long numCols,
      StorageType storageType) {
    LongLongVector[] rows = new LongLongVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyLongVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyLongVector(numCols);
          break;
      }
    }

    return new RBLongLongMatrix(rows);
  }

  public static RBLongLongMatrix rbLongLongMatrix(LongLongVector[] rows) {
    return new RBLongLongMatrix(rows);
  }

  public static RBLongLongMatrix rbLongLongMatrix(int numRows, long numCols) {
    return new RBLongLongMatrix(numRows, numCols);
  }

  public static RBLongIntMatrix rbLongIntMatrix(int matrixId, int clock, LongIntVector[] rows) {
    return new RBLongIntMatrix(matrixId, clock, rows);
  }

  public static RBLongIntMatrix rbLongIntMatrix(int matrixId, int clock, int numRows,
      long numCols) {
    return new RBLongIntMatrix(matrixId, clock, numRows, numCols);
  }

  public static RBLongIntMatrix rbLongIntMatrix(int matrixId, int clock, int numRows, long numCols,
      StorageType storageType) {
    LongIntVector[] rows = new LongIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyIntVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyIntVector(numCols);
          break;
      }
    }

    return new RBLongIntMatrix(matrixId, clock, rows);
  }

  public static RBLongIntMatrix rbLongIntMatrix(int numRows, long numCols,
      StorageType storageType) {
    LongIntVector[] rows = new LongIntVector[numRows];
    for (int i = 0; i < numRows; i++) {
      switch (storageType) {
        case DENSE:
          throw new AngelException("DENSE long key is not supported!");
        case SPARSE:
          rows[i] = VFactory.sparseLongKeyIntVector(numCols);
          break;
        case SORTED:
          rows[i] = VFactory.sortedLongKeyIntVector(numCols);
          break;
      }
    }

    return new RBLongIntMatrix(rows);
  }

  public static RBLongIntMatrix rbLongIntMatrix(LongIntVector[] rows) {
    return new RBLongIntMatrix(rows);
  }

  public static RBLongIntMatrix rbLongIntMatrix(int numRows, long numCols) {
    return new RBLongIntMatrix(numRows, numCols);
  }

  //---------------------------------------------------

  public static BlasDoubleMatrix denseDoubleMatrix(int matrixId, int clock, int numRows,
      int numCols, double[] data) {
    return new BlasDoubleMatrix(matrixId, clock, numRows, numCols, data);
  }

  public static BlasDoubleMatrix denseDoubleMatrix(int matrixId, int clock, int numRows,
      int numCols) {
    double[] data = new double[numRows * numCols];
    return new BlasDoubleMatrix(matrixId, clock, numRows, numCols, data);
  }

  public static BlasDoubleMatrix denseDoubleMatrix(int numRows, int numCols, double[] data) {
    return new BlasDoubleMatrix(numRows, numCols, data);
  }

  public static BlasDoubleMatrix denseDoubleMatrix(int numRows, int numCols) {
    double[] data = new double[numRows * numCols];
    return new BlasDoubleMatrix(numRows, numCols, data);
  }

  public static BlasFloatMatrix denseFloatMatrix(int matrixId, int clock, int numRows, int numCols,
      float[] data) {
    return new BlasFloatMatrix(matrixId, clock, numRows, numCols, data);
  }

  public static BlasFloatMatrix denseFloatMatrix(int matrixId, int clock, int numRows,
      int numCols) {
    float[] data = new float[numRows * numCols];
    return new BlasFloatMatrix(matrixId, clock, numRows, numCols, data);
  }

  public static BlasFloatMatrix denseFloatMatrix(int numRows, int numCols, float[] data) {
    return new BlasFloatMatrix(numRows, numCols, data);
  }

  public static BlasFloatMatrix denseFloatMatrix(int numRows, int numCols) {
    float[] data = new float[numRows * numCols];
    return new BlasFloatMatrix(numRows, numCols, data);
  }

  public static MapMatrix<IntDoubleVector> intDoubleMapMatrix(int matrixId, int clock,
      HashMap<Long, IntDoubleVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntFloatVector> intFloatMapMatrix(int matrixId, int clock,
      HashMap<Long, IntFloatVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntLongVector> intLongMapMatrix(int matrixId, int clock,
      HashMap<Long, IntLongVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntIntVector> intIntMapMatrix(int matrixId, int clock,
      HashMap<Long, IntIntVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongDoubleVector> longDoubleMapMatrix(int matrixId, int clock,
      HashMap<Long, LongDoubleVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongFloatVector> longFloatMapMatrix(int matrixId, int clock,
      HashMap<Long, LongFloatVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongLongVector> longLongMapMatrix(int matrixId, int clock,
      HashMap<Long, LongLongVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongIntVector> longIntMapMatrix(int matrixId, int clock,
      HashMap<Long, LongIntVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntDoubleVector> intDoubleMapMatrix(
      HashMap<Integer, IntDoubleVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntFloatVector> intFloatMapMatrix(
      HashMap<Integer, IntFloatVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntLongVector> intLongMapMatrix(
      HashMap<Integer, IntLongVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntIntVector> intIntMapMatrix(HashMap<Integer, IntIntVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongDoubleVector> longDoubleMapMatrix(
      HashMap<Long, LongDoubleVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongFloatVector> longFloatMapMatrix(
      HashMap<Long, LongFloatVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongLongVector> longLongMapMatrix(
      HashMap<Long, LongLongVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongIntVector> longIntMapMatrix(HashMap<Long, LongIntVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntDoubleVector> compIntDoubleMapMatrix(int matrixId, int clock,
      HashMap<Long, CompIntDoubleVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntFloatVector> compIntFloatMapMatrix(int matrixId, int clock,
      HashMap<Long, CompIntFloatVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntLongVector> compIntLongMapMatrix(int matrixId, int clock,
      HashMap<Long, CompIntLongVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntIntVector> compIntIntMapMatrix(int matrixId, int clock,
      HashMap<Long, CompIntIntVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongDoubleVector> compLongDoubleMapMatrix(int matrixId, int clock,
      HashMap<Long, CompLongDoubleVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongFloatVector> compLongFloatMapMatrix(int matrixId, int clock,
      HashMap<Long, CompLongFloatVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongLongVector> compLongLongMapMatrix(int matrixId, int clock,
      HashMap<Long, CompLongLongVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<LongIntVector> compLongIntMapMatrix(int matrixId, int clock,
      HashMap<Long, CompLongIntVector> mapMatrix) {
    return new MapMatrix(matrixId, clock, mapMatrix);
  }

  public static MapMatrix<IntDoubleVector> compIntDoubleMapMatrix(
      HashMap<Integer, CompIntDoubleVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntFloatVector> compIntFloatMapMatrix(
      HashMap<Integer, CompIntFloatVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntLongVector> compIntLongMapMatrix(
      HashMap<Integer, CompIntLongVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<IntIntVector> compIntIntMapMatrix(
      HashMap<Integer, CompIntIntVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongDoubleVector> compLongDoubleMapMatrix(
      HashMap<Long, CompLongDoubleVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongFloatVector> compLongFloatMapMatrix(
      HashMap<Long, CompLongFloatVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongLongVector> compLongLongMapMatrix(
      HashMap<Long, CompLongLongVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static MapMatrix<LongIntVector> compLongIntMapMatrix(
      HashMap<Long, CompLongIntVector> mapMatrix) {
    return new MapMatrix(mapMatrix);
  }

  public static CooIntDoubleMatrix cooIntDoubleMatrix(
      int matrixId, int clock, int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    return new CooIntDoubleMatrix(matrixId, clock, rowIndices, colIndices, values, shape);
  }

  public static CooIntDoubleMatrix cooIntDoubleMatrix(
      int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    return new CooIntDoubleMatrix(0, 0, rowIndices, colIndices, values, shape);
  }

  public static CooIntFloatMatrix cooIntFloatMatrix(
      int matrixId, int clock, int[] rowIndices, int[] colIndices, float[] values, int[] shape) {
    return new CooIntFloatMatrix(matrixId, clock, rowIndices, colIndices, values, shape);
  }

  public static CooIntFloatMatrix cooIntFloatMatrix(
      int[] rowIndices, int[] colIndices, float[] values, int[] shape) {
    return new CooIntFloatMatrix(rowIndices, colIndices, values, shape);
  }

  public static CooLongFloatMatrix cooLongFloatMatrix(
      long[] rowIndices, long[] colIndices, float[] values, int[] shape) {
    return new CooLongFloatMatrix(rowIndices, colIndices, values, shape);
  }

  public static CooLongFloatMatrix cooLongFloatMatrix(
      int matrixId, int clock, long[] rowIndices, long[] colIndices, float[] values, int[] shape) {
    return new CooLongFloatMatrix(matrixId, clock, rowIndices, colIndices, values, shape);
  }

  public static CooLongDoubleMatrix cooLongDoubleMatrix(
      long[] rowIndices, long[] colIndices, double[] values, int[] shape) {
    return new CooLongDoubleMatrix(rowIndices, colIndices, values, shape);
  }

  public static CooLongDoubleMatrix cooLongDoubleMatrix(
      int matrixId, int clock, long[] rowIndices, long[] colIndices, double[] values, int[] shape) {
    return new CooLongDoubleMatrix(matrixId, clock, rowIndices, colIndices, values, shape);
  }




  public static CsrDoubleMatrix csrDoubleMatrix(
      int matrixId, int clock, int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    return new CsrDoubleMatrix(matrixId, clock, rowIndices, colIndices, values, shape);
  }

  public static CsrDoubleMatrix csrDoubleMatrix(
      int[] rowIndices, int[] colIndices, double[] values, int[] shape) {
    return new CsrDoubleMatrix(0, 0, rowIndices, colIndices, values, shape);
  }

  public static CsrFloatMatrix csrFloatMatrix(
      int matrixId, int clock, int[] rowIndices, int[] colIndices, float[] values, int[] shape) {
    return new CsrFloatMatrix(matrixId, clock, rowIndices, colIndices, values, shape);
  }

  public static CsrFloatMatrix csrFloatMatrix(
      int[] rowIndices, int[] colIndices, float[] values, int[] shape) {
    return new CsrFloatMatrix(rowIndices, colIndices, values, shape);
  }

  public static CsrDoubleMatrix csrDoubleMatrix(
      int matrixId, int clock, double[] values, int[] indices, int[] indptr, int[] shape) {
    return new CsrDoubleMatrix(matrixId, clock, values, indices, indptr, shape);
  }

  public static CsrDoubleMatrix csrDoubleMatrix(
      double[] values, int[] indices, int[] indptr, int[] shape) {
    return new CsrDoubleMatrix(0, 0,values,  indices, indptr, shape);
  }

  public static CsrFloatMatrix csrFloatMatrix(
      int matrixId, int clock, float[] values, int[] indices, int[] indptr, int[] shape) {
    return new CsrFloatMatrix(matrixId, clock, values, indices, indptr, shape);
  }

  public static CsrFloatMatrix csrFloatMatrix(
      float[] values, int[] indices, int[] indptr, int[] shape) {
    return new CsrFloatMatrix(values, indices, indptr, shape);
  }
}
