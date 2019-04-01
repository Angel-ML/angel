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
import com.tencent.angel.ml.math2.ufuncs.executor.DotExecutor;
import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.vector.CompIntDoubleVector;
import com.tencent.angel.ml.math2.vector.CompIntFloatVector;
import com.tencent.angel.ml.math2.vector.CompIntIntVector;
import com.tencent.angel.ml.math2.vector.CompIntLongVector;
import com.tencent.angel.ml.math2.vector.CompLongDoubleVector;
import com.tencent.angel.ml.math2.vector.CompLongFloatVector;
import com.tencent.angel.ml.math2.vector.CompLongIntVector;
import com.tencent.angel.ml.math2.vector.CompLongLongVector;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;


public class CompDotExecutor {

  private static final int THREADS = ForkJoinUtils.getNCores();
  private static ForkJoinPool pool = ForkJoinUtils.getPool();

  public static double apply(ComponentVector v1, ComponentVector v2) {
    CompDotExe task = new CompDotExe(v1, v2, 0, v1.getNumPartitions() - 1);
    Future<Double> result = pool.submit(task);

    try {
      return result.get();
    } catch (InterruptedException | ExecutionException e) {
      e.printStackTrace();
    }

    return Double.NaN;
  }

  private static double apply(CompIntDoubleVector v1, CompIntDoubleVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntDoubleVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntDoubleVector v1, CompIntFloatVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntFloatVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntDoubleVector v1, CompIntLongVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntLongVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntDoubleVector v1, CompIntIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntFloatVector v1, CompIntFloatVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntFloatVector[] v1Parts = v1.getPartitions();
      IntFloatVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntFloatVector v1, CompIntLongVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntFloatVector[] v1Parts = v1.getPartitions();
      IntLongVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntFloatVector v1, CompIntIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntFloatVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntLongVector v1, CompIntLongVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntLongVector[] v1Parts = v1.getPartitions();
      IntLongVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntLongVector v1, CompIntIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntLongVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompIntIntVector v1, CompIntIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      IntIntVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongDoubleVector v1, CompLongDoubleVector v2, int start,
      int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongDoubleVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongDoubleVector v1, CompLongFloatVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongFloatVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongDoubleVector v1, CompLongLongVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongLongVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongDoubleVector v1, CompLongIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongFloatVector v1, CompLongFloatVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongFloatVector[] v1Parts = v1.getPartitions();
      LongFloatVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongFloatVector v1, CompLongLongVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongFloatVector[] v1Parts = v1.getPartitions();
      LongLongVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongFloatVector v1, CompLongIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongFloatVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongLongVector v1, CompLongLongVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongLongVector[] v1Parts = v1.getPartitions();
      LongLongVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongLongVector v1, CompLongIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongLongVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static double apply(CompLongIntVector v1, CompLongIntVector v2, int start, int end) {
    double dot = 0.0;
    if (v1.isCompatable(v2)) {
      LongIntVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      for (int i = start; i <= end; i++) {
        dot += DotExecutor.apply(v1Parts[i], v2Parts[i]);
      }

      return dot;
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static class CompDotExe extends RecursiveTask<Double> {

    private int start, end, threshold;
    private ComponentVector v1, v2;

    public CompDotExe(ComponentVector v1, ComponentVector v2, int start, int end) {
      assert v1 != null && v2 != null;
      this.start = start;
      this.end = end;
      this.threshold = (v1.getNumPartitions() + THREADS - 1) / THREADS;
      this.v1 = v1;
      this.v2 = v2;
    }


    @Override
    protected Double compute() {
      double dot = Double.NaN;
      boolean canCompute = (end - start) < threshold;

      if (canCompute) {
        if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntDoubleVector) {
          dot = apply((CompIntDoubleVector) v1, (CompIntDoubleVector) v2, start, end);
        } else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntFloatVector) {
          dot = apply((CompIntDoubleVector) v1, (CompIntFloatVector) v2, start, end);
        } else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntLongVector) {
          dot = apply((CompIntDoubleVector) v1, (CompIntLongVector) v2, start, end);
        } else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntIntVector) {
          dot = apply((CompIntDoubleVector) v1, (CompIntIntVector) v2, start, end);
        } else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntFloatVector) {
          dot = apply((CompIntFloatVector) v1, (CompIntFloatVector) v2, start, end);
        } else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntLongVector) {
          dot = apply((CompIntFloatVector) v1, (CompIntLongVector) v2, start, end);
        } else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntIntVector) {
          dot = apply((CompIntFloatVector) v1, (CompIntIntVector) v2, start, end);
        } else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntLongVector) {
          dot = apply((CompIntLongVector) v1, (CompIntLongVector) v2, start, end);
        } else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntIntVector) {
          dot = apply((CompIntLongVector) v1, (CompIntIntVector) v2, start, end);
        } else if (v1 instanceof CompIntIntVector && v2 instanceof CompIntIntVector) {
          dot = apply((CompIntIntVector) v1, (CompIntIntVector) v2, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongDoubleVector) {
          dot = apply((CompLongDoubleVector) v1, (CompLongDoubleVector) v2, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongFloatVector) {
          dot = apply((CompLongDoubleVector) v1, (CompLongFloatVector) v2, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongLongVector) {
          dot = apply((CompLongDoubleVector) v1, (CompLongLongVector) v2, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongIntVector) {
          dot = apply((CompLongDoubleVector) v1, (CompLongIntVector) v2, start, end);
        } else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongFloatVector) {
          dot = apply((CompLongFloatVector) v1, (CompLongFloatVector) v2, start, end);
        } else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongLongVector) {
          dot = apply((CompLongFloatVector) v1, (CompLongLongVector) v2, start, end);
        } else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongIntVector) {
          dot = apply((CompLongFloatVector) v1, (CompLongIntVector) v2, start, end);
        } else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongLongVector) {
          dot = apply((CompLongLongVector) v1, (CompLongLongVector) v2, start, end);
        } else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongIntVector) {
          dot = apply((CompLongLongVector) v1, (CompLongIntVector) v2, start, end);
        } else if (v1 instanceof CompLongIntVector && v2 instanceof CompLongIntVector) {
          dot = apply((CompLongIntVector) v1, (CompLongIntVector) v2, start, end);
        } else {
          throw new AngelException("The operation is not support!");
        }
      } else {
        int middle = (start + end) >> 1;

        CompDotExe left = new CompDotExe(v1, v2, start, middle);
        CompDotExe right = new CompDotExe(v1, v2, middle + 1, end);

        left.fork();
        right.fork();

        double leftRes = left.join();
        double rightRes = right.join();

        dot = leftRes + rightRes;
      }

      return dot;
    }
  }

}