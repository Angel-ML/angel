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
import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.utils.VectorUtils;
import com.tencent.angel.ml.math2.vector.*;

import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class CompBinaryExecutor {
  private static ForkJoinPool pool = ForkJoinUtils.getPool();
  private static final int THREADS = ForkJoinUtils.getNCores();

  public static Vector apply(ComponentVector v1, ComponentVector v2, Binary op) {
    ComponentVector result;

    if (op.isInplace()) {
      result = v1;
    } else {
      result = (ComponentVector) VectorUtils.emptyLike((Vector) v1);
    }

    CompBinExe task = new CompBinExe(v1, v2, op, result, 0, v1.getNumPartitions() - 1);
    pool.execute(task);
    task.join();

    return (Vector) result;
  }

  private static class CompBinExe extends RecursiveAction {
    private ComponentVector v1, v2, result;
    private int start, end, threshold;
    private Binary op;

    public CompBinExe(ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result,
      int start, int end) {
      this.v1 = v1;
      this.v2 = v2;
      this.result = result;
      this.start = start;
      this.end = end;
      this.threshold = (v1.getNumPartitions() + THREADS - 1) / THREADS;
      this.op = op;
    }

    @Override protected void compute() {
      boolean canCompute = (end - start) < threshold;

      if (canCompute) {
        if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntDoubleVector) {
          apply((CompIntDoubleVector) v1, (CompIntDoubleVector) v2, op,
            (CompIntDoubleVector) result, start, end);
        } else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntFloatVector) {
          apply((CompIntDoubleVector) v1, (CompIntFloatVector) v2, op, (CompIntDoubleVector) result,
            start, end);
        } else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntLongVector) {
          apply((CompIntDoubleVector) v1, (CompIntLongVector) v2, op, (CompIntDoubleVector) result,
            start, end);
        } else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntIntVector) {
          apply((CompIntDoubleVector) v1, (CompIntIntVector) v2, op, (CompIntDoubleVector) result,
            start, end);
        } else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntFloatVector) {
          apply((CompIntFloatVector) v1, (CompIntFloatVector) v2, op, (CompIntFloatVector) result,
            start, end);
        } else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntLongVector) {
          apply((CompIntFloatVector) v1, (CompIntLongVector) v2, op, (CompIntFloatVector) result,
            start, end);
        } else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntIntVector) {
          apply((CompIntFloatVector) v1, (CompIntIntVector) v2, op, (CompIntFloatVector) result,
            start, end);
        } else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntLongVector) {
          apply((CompIntLongVector) v1, (CompIntLongVector) v2, op, (CompIntLongVector) result,
            start, end);
        } else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntIntVector) {
          apply((CompIntLongVector) v1, (CompIntIntVector) v2, op, (CompIntLongVector) result,
            start, end);
        } else if (v1 instanceof CompIntIntVector && v2 instanceof CompIntIntVector) {
          apply((CompIntIntVector) v1, (CompIntIntVector) v2, op, (CompIntIntVector) result, start,
            end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongDoubleVector) {
          apply((CompLongDoubleVector) v1, (CompLongDoubleVector) v2, op,
            (CompLongDoubleVector) result, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongFloatVector) {
          apply((CompLongDoubleVector) v1, (CompLongFloatVector) v2, op,
            (CompLongDoubleVector) result, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongLongVector) {
          apply((CompLongDoubleVector) v1, (CompLongLongVector) v2, op,
            (CompLongDoubleVector) result, start, end);
        } else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongIntVector) {
          apply((CompLongDoubleVector) v1, (CompLongIntVector) v2, op,
            (CompLongDoubleVector) result, start, end);
        } else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongFloatVector) {
          apply((CompLongFloatVector) v1, (CompLongFloatVector) v2, op,
            (CompLongFloatVector) result, start, end);
        } else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongLongVector) {
          apply((CompLongFloatVector) v1, (CompLongLongVector) v2, op, (CompLongFloatVector) result,
            start, end);
        } else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongIntVector) {
          apply((CompLongFloatVector) v1, (CompLongIntVector) v2, op, (CompLongFloatVector) result,
            start, end);
        } else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongLongVector) {
          apply((CompLongLongVector) v1, (CompLongLongVector) v2, op, (CompLongLongVector) result,
            start, end);
        } else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongIntVector) {
          apply((CompLongLongVector) v1, (CompLongIntVector) v2, op, (CompLongLongVector) result,
            start, end);
        } else if (v1 instanceof CompLongIntVector && v2 instanceof CompLongIntVector) {
          apply((CompLongIntVector) v1, (CompLongIntVector) v2, op, (CompLongIntVector) result,
            start, end);
        } else {
          throw new AngelException("The operation is not support!");
        }
      } else {
        int middle = (start + end) >> 1;

        CompBinExe left = new CompBinExe(v1, v2, op, result, start, middle);
        CompBinExe right = new CompBinExe(v1, v2, op, result, middle + 1, end);

        invokeAll(left, right);
      }
    }
  }

  private static void apply(CompIntDoubleVector v1, CompIntDoubleVector v2, Binary op,
    CompIntDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntDoubleVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntDoubleVector v1, CompIntFloatVector v2, Binary op,
    CompIntDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntFloatVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntDoubleVector v1, CompIntLongVector v2, Binary op,
    CompIntDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntLongVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntDoubleVector v1, CompIntIntVector v2, Binary op,
    CompIntDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntDoubleVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntFloatVector v1, CompIntFloatVector v2, Binary op,
    CompIntFloatVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntFloatVector[] v1Parts = v1.getPartitions();
      IntFloatVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntFloatVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntFloatVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntFloatVector v1, CompIntLongVector v2, Binary op,
    CompIntFloatVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntFloatVector[] v1Parts = v1.getPartitions();
      IntLongVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntFloatVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntFloatVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntFloatVector v1, CompIntIntVector v2, Binary op,
    CompIntFloatVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntFloatVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntFloatVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntFloatVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntLongVector v1, CompIntLongVector v2, Binary op,
    CompIntLongVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntLongVector[] v1Parts = v1.getPartitions();
      IntLongVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntLongVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntLongVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntLongVector v1, CompIntIntVector v2, Binary op,
    CompIntLongVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntLongVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntLongVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntLongVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompIntIntVector v1, CompIntIntVector v2, Binary op,
    CompIntIntVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      IntIntVector[] v1Parts = v1.getPartitions();
      IntIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        IntIntVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (IntIntVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongDoubleVector v1, CompLongDoubleVector v2, Binary op,
    CompLongDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongDoubleVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongDoubleVector v1, CompLongFloatVector v2, Binary op,
    CompLongDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongFloatVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongDoubleVector v1, CompLongLongVector v2, Binary op,
    CompLongDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongLongVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongDoubleVector v1, CompLongIntVector v2, Binary op,
    CompLongDoubleVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongDoubleVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongDoubleVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongDoubleVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongFloatVector v1, CompLongFloatVector v2, Binary op,
    CompLongFloatVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongFloatVector[] v1Parts = v1.getPartitions();
      LongFloatVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongFloatVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongFloatVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongFloatVector v1, CompLongLongVector v2, Binary op,
    CompLongFloatVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongFloatVector[] v1Parts = v1.getPartitions();
      LongLongVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongFloatVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongFloatVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongFloatVector v1, CompLongIntVector v2, Binary op,
    CompLongFloatVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongFloatVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongFloatVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongFloatVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongLongVector v1, CompLongLongVector v2, Binary op,
    CompLongLongVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongLongVector[] v1Parts = v1.getPartitions();
      LongLongVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongLongVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongLongVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongLongVector v1, CompLongIntVector v2, Binary op,
    CompLongLongVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongLongVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongLongVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongLongVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }

  private static void apply(CompLongIntVector v1, CompLongIntVector v2, Binary op,
    CompLongIntVector result, int start, int end) {
    if (v1.isCompatable(v2)) {
      LongIntVector[] v1Parts = v1.getPartitions();
      LongIntVector[] v2Parts = v2.getPartitions();
      if (op.isInplace()) {
        for (int i = start; i <= end; i++) {
          BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      } else {
        LongIntVector[] resParts = result.getPartitions();
        for (int i = start; i <= end; i++) {
          resParts[i] = (LongIntVector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);
        }
      }
    } else {
      throw new AngelException("Operation is not support!");
    }
  }


}
