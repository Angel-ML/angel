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


package com.tencent.angel.ml.math2.ufuncs.executor.comp;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.utils.VectorUtils;
import com.tencent.angel.ml.math2.vector.*;

import java.util.concurrent.*;


public class CompUnaryExecutor {
  private static ForkJoinPool pool = ForkJoinUtils.getPool();
  private static final int THREADS = ForkJoinUtils.getNCores();

  public static Vector apply(ComponentVector v1, Unary op) {
    ComponentVector result;

    if (op.isInplace()) {
      result = v1;
    } else {
      result = (ComponentVector) VectorUtils.emptyLike((Vector) v1);
    }

    CompUnaExe task = new CompUnaExe(v1, op, result, 0, v1.getNumPartitions() - 1);
    pool.execute(task);
    task.join();

    return (Vector) result;
  }

  private static class CompUnaExe extends RecursiveAction {
    int start, end, threshold;
    ComponentVector v, result;
    Unary op;

    public CompUnaExe(ComponentVector v, Unary op, ComponentVector result, int start, int end) {
      assert v != null && op != null;
      this.start = start;
      this.end = end;
      this.threshold = (v.getNumPartitions() + THREADS - 1) / THREADS;
      ;
      this.v = v;
      this.result = result;
      this.op = op;
    }

    @Override protected void compute() {
      boolean canCompute = (end - start) < threshold;

      if (canCompute) {
        if (v instanceof CompIntDoubleVector) {
          apply((CompIntDoubleVector) v, op, (CompIntDoubleVector) result, start, end);
        } else if (v instanceof CompIntFloatVector) {
          apply((CompIntFloatVector) v, op, (CompIntFloatVector) result, start, end);
        } else if (v instanceof CompIntLongVector) {
          apply((CompIntLongVector) v, op, (CompIntLongVector) result, start, end);
        } else if (v instanceof CompIntIntVector) {
          apply((CompIntIntVector) v, op, (CompIntIntVector) result, start, end);
        } else if (v instanceof CompLongDoubleVector) {
          apply((CompLongDoubleVector) v, op, (CompLongDoubleVector) result, start, end);
        } else if (v instanceof CompLongFloatVector) {
          apply((CompLongFloatVector) v, op, (CompLongFloatVector) result, start, end);
        } else if (v instanceof CompLongLongVector) {
          apply((CompLongLongVector) v, op, (CompLongLongVector) result, start, end);
        } else if (v instanceof CompLongIntVector) {
          apply((CompLongIntVector) v, op, (CompLongIntVector) result, start, end);
        } else {
          throw new AngelException("Vector type is not support!");
        }
      } else {
        int middle = (start + end) >> 1;

        CompUnaExe left = new CompUnaExe(v, op, result, start, middle);
        CompUnaExe right = new CompUnaExe(v, op, result, middle + 1, end);

        invokeAll(left, right);
      }
    }
  }

  private static void apply(CompIntDoubleVector v, Unary op, CompIntDoubleVector result, int start,
    int end) {
    IntDoubleVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      IntDoubleVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (IntDoubleVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompIntFloatVector v, Unary op, CompIntFloatVector result, int start,
    int end) {
    IntFloatVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      IntFloatVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (IntFloatVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompIntLongVector v, Unary op, CompIntLongVector result, int start,
    int end) {
    IntLongVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      IntLongVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (IntLongVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompIntIntVector v, Unary op, CompIntIntVector result, int start,
    int end) {
    IntIntVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      IntIntVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (IntIntVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompLongDoubleVector v, Unary op, CompLongDoubleVector result,
    int start, int end) {
    LongDoubleVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      LongDoubleVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (LongDoubleVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompLongFloatVector v, Unary op, CompLongFloatVector result, int start,
    int end) {
    LongFloatVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      LongFloatVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (LongFloatVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompLongLongVector v, Unary op, CompLongLongVector result, int start,
    int end) {
    LongLongVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      LongLongVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (LongLongVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }

  private static void apply(CompLongIntVector v, Unary op, CompLongIntVector result, int start,
    int end) {
    LongIntVector[] v1Parts = v.getPartitions();

    if (op.isInplace()) {
      for (int i = start; i <= end; i++) {
        UnaryExecutor.apply(v1Parts[i], op);
      }
    } else {
      LongIntVector[] resParts = result.getPartitions();
      for (int i = start; i <= end; i++) {
        resParts[i] = (LongIntVector) UnaryExecutor.apply(v1Parts[i], op);
      }
    }
  }


}