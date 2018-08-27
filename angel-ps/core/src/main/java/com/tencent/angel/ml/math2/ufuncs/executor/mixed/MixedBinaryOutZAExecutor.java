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


package com.tencent.angel.ml.math2.ufuncs.executor.mixed;

import com.tencent.angel.exception.AngelException;
import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class MixedBinaryOutZAExecutor {
  public static Vector apply(ComponentVector v1, Vector v2, Binary op) {
    if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDoubleVector) {
      return apply((CompIntDoubleVector) v1, (IntDoubleVector) v2, op);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntFloatVector) {
      return apply((CompIntDoubleVector) v1, (IntFloatVector) v2, op);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntLongVector) {
      return apply((CompIntDoubleVector) v1, (IntLongVector) v2, op);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntIntVector) {
      return apply((CompIntDoubleVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDummyVector) {
      return apply((CompIntDoubleVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntFloatVector) {
      return apply((CompIntFloatVector) v1, (IntFloatVector) v2, op);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntLongVector) {
      return apply((CompIntFloatVector) v1, (IntLongVector) v2, op);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntIntVector) {
      return apply((CompIntFloatVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntDummyVector) {
      return apply((CompIntFloatVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntLongVector) {
      return apply((CompIntLongVector) v1, (IntLongVector) v2, op);
    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntIntVector) {
      return apply((CompIntLongVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntDummyVector) {
      return apply((CompIntLongVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof CompIntIntVector && v2 instanceof IntIntVector) {
      return apply((CompIntIntVector) v1, (IntIntVector) v2, op);
    } else if (v1 instanceof CompIntIntVector && v2 instanceof IntDummyVector) {
      return apply((CompIntIntVector) v1, (IntDummyVector) v2, op);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDoubleVector) {
      return apply((CompLongDoubleVector) v1, (LongDoubleVector) v2, op);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongFloatVector) {
      return apply((CompLongDoubleVector) v1, (LongFloatVector) v2, op);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongLongVector) {
      return apply((CompLongDoubleVector) v1, (LongLongVector) v2, op);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongIntVector) {
      return apply((CompLongDoubleVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDummyVector) {
      return apply((CompLongDoubleVector) v1, (LongDummyVector) v2, op);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongFloatVector) {
      return apply((CompLongFloatVector) v1, (LongFloatVector) v2, op);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongLongVector) {
      return apply((CompLongFloatVector) v1, (LongLongVector) v2, op);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongIntVector) {
      return apply((CompLongFloatVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongDummyVector) {
      return apply((CompLongFloatVector) v1, (LongDummyVector) v2, op);
    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongLongVector) {
      return apply((CompLongLongVector) v1, (LongLongVector) v2, op);
    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongIntVector) {
      return apply((CompLongLongVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongDummyVector) {
      return apply((CompLongLongVector) v1, (LongDummyVector) v2, op);
    } else if (v1 instanceof CompLongIntVector && v2 instanceof LongIntVector) {
      return apply((CompLongIntVector) v1, (LongIntVector) v2, op);
    } else if (v1 instanceof CompLongIntVector && v2 instanceof LongDummyVector) {
      return apply((CompLongIntVector) v1, (LongDummyVector) v2, op);
    } else {
      throw new AngelException("Vector type is not support!");
    }
  }


  private static Vector apply(CompIntDoubleVector v1, IntDummyVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      IntDoubleVector part = parts[i];
      resParts[i] =
        new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    int[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      int idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      int subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntDoubleVector v1, IntDoubleVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
      int base = 0;
      double[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVector resPart = part.copy();
        if (part.isDense()) {
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getDoubleValue()));
          }
        }
        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
      int base = 0;
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVector resPart = part.copy();
        if (part.isDense()) {
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVector resPart = part.copy();
        if (part.isDense()) {
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVector resPart = part.copy();
        if (part.isDense()) {
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          double[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          resParts[i] =
            new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntDoubleVector resVect = v1.copy();
        IntDoubleVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    IntFloatVector[] resParts = new IntFloatVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      IntFloatVector part = parts[i];
      resParts[i] =
        new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    int[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      int idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      int subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntFloatVector[] resParts = new IntFloatVector[parts.length];
      int base = 0;
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVector resPart = part.copy();
        if (part.isDense()) {
          float[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          float[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntFloatVector[] resParts = new IntFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          resParts[i] =
            new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
        return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntFloatVector resVect = v1.copy();
        IntFloatVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntFloatVector[] resParts = new IntFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          resParts[i] =
            new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntFloatVector resVect = v1.copy();
        IntFloatVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntFloatVector[] resParts = new IntFloatVector[parts.length];
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVector resPart = part.copy();
        if (part.isDense()) {
          float[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          float[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntFloatVector[] resParts = new IntFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          resParts[i] =
            new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
        return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntFloatVector resVect = v1.copy();
        IntFloatVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntFloatVector[] resParts = new IntFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          resParts[i] =
            new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntFloatVector resVect = v1.copy();
        IntFloatVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntFloatVector[] resParts = new IntFloatVector[parts.length];
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVector resPart = part.copy();
        if (part.isDense()) {
          float[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          float[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntFloatVector[] resParts = new IntFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          resParts[i] =
            new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntFloatVector resVect = v1.copy();
        IntFloatVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntFloatVector[] resParts = new IntFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          resParts[i] =
            new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntFloatVector resVect = v1.copy();
        IntFloatVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    IntLongVector[] resParts = new IntLongVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      IntLongVector part = parts[i];
      resParts[i] =
        new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    int[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      int idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      int subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntLongVector[] resParts = new IntLongVector[parts.length];
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntLongVector part = parts[i];
        IntLongVector resPart = part.copy();
        if (part.isDense()) {
          long[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          long[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntLongVector[] resParts = new IntLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          resParts[i] =
            new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
        return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntLongVector resVect = v1.copy();
        IntLongVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntLongVector[] resParts = new IntLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          resParts[i] =
            new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntLongVector resVect = v1.copy();
        IntLongVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntLongVector[] resParts = new IntLongVector[parts.length];
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntLongVector part = parts[i];
        IntLongVector resPart = part.copy();
        if (part.isDense()) {
          long[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          long[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntLongVector[] resParts = new IntLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          resParts[i] =
            new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntLongVector resVect = v1.copy();
        IntLongVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntLongVector[] resParts = new IntLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          resParts[i] =
            new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntLongVector resVect = v1.copy();
        IntLongVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompIntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVector[] parts = v1.getPartitions();
    IntIntVector[] resParts = new IntIntVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      IntIntVector part = parts[i];
      resParts[i] =
        new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    int[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      int idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      int subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVector[] parts = v1.getPartitions();
    if (v2.isDense()) {
      IntIntVector[] resParts = new IntIntVector[parts.length];
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntIntVector part = parts[i];
        IntIntVector resPart = part.copy();
        if (part.isDense()) {
          int[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartValues.length; j++) {
            resPartValues[j] = op.apply(resPartValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2IntMap.Entry> iter = resPart.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey() + base;
            entry.setValue(op.apply(entry.getIntValue(), v2Values[idx]));
          }
        } else { // sorted
          int[] resPartIndices = resPart.getStorage().getIndices();
          int[] resPartValues = resPart.getStorage().getValues();
          for (int j = 0; j < resPartIndices.length; j++) {
            int idx = resPartIndices[j] + base;
            resPartValues[j] = op.apply(resPartValues[j], v2Values[idx]);
          }
        }
        resParts[i] = resPart;
        base += part.getDim();
      }
      return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        resParts, v1.getSubDim());
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        IntIntVector[] resParts = new IntIntVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntIntVector part = parts[i];
          resParts[i] =
            new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntIntVector resVect = v1.copy();
        IntIntVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntIntVector part : resParts) {
          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2IntMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        IntIntVector[] resParts = new IntIntVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          IntIntVector part = parts[i];
          resParts[i] =
            new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompIntIntVector resVect = v1.copy();
        IntIntVector[] resParts = resVect.getPartitions();
        int base = 0;
        for (IntIntVector part : resParts) {
          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2IntMap.Entry entry = piter.next();
              int idx = entry.getIntKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            int[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              int idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      LongDoubleVector part = parts[i];
      resParts[i] =
        new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    long[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      long idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      long subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getDoubleValue()));
          }
        }
        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          resParts[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
            part.getDim(), part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongDoubleVector resVect = v1.copy();
        LongDoubleVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongDoubleVector part : resParts) {
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getDoubleValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    LongFloatVector[] resParts = new LongFloatVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      LongFloatVector part = parts[i];
      resParts[i] =
        new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    long[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      long idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      long subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongFloatVector[] resParts = new LongFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          resParts[i] =
            new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
        return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongFloatVector resVect = v1.copy();
        LongFloatVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongFloatVector[] resParts = new LongFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          resParts[i] =
            new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongFloatVector resVect = v1.copy();
        LongFloatVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongFloatVector[] resParts = new LongFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          resParts[i] =
            new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
        return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongFloatVector resVect = v1.copy();
        LongFloatVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongFloatVector[] resParts = new LongFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          resParts[i] =
            new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongFloatVector resVect = v1.copy();
        LongFloatVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongFloatVector[] resParts = new LongFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          resParts[i] =
            new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongFloatVector resVect = v1.copy();
        LongFloatVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongFloatVector[] resParts = new LongFloatVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          resParts[i] =
            new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongFloatVector resVect = v1.copy();
        LongFloatVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongFloatVector part : resParts) {
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getFloatValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    LongLongVector[] resParts = new LongLongVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      LongLongVector part = parts[i];
      resParts[i] =
        new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    long[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      long idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      long subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongLongVector[] resParts = new LongLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          resParts[i] =
            new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
        return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongLongVector resVect = v1.copy();
        LongLongVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongLongVector[] resParts = new LongLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          resParts[i] =
            new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongLongVector resVect = v1.copy();
        LongLongVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongLongVector[] resParts = new LongLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          resParts[i] =
            new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongLongVector resVect = v1.copy();
        LongLongVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongLongVector[] resParts = new LongLongVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          resParts[i] =
            new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongLongVector resVect = v1.copy();
        LongLongVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongLongVector part : resParts) {
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getLongValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


  private static Vector apply(CompLongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVector[] parts = v1.getPartitions();
    LongIntVector[] resParts = new LongIntVector[parts.length];
    for (int i = 0; i < parts.length; i++) {
      LongIntVector part = parts[i];
      resParts[i] =
        new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          part.getStorage().emptySparse());
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
    long[] v2Indices = v2.getIndices();
    for (int i = 0; i < v2Indices.length; i++) {
      long idx = v2Indices[i];
      int pidx = (int) (idx / subDim);
      long subidx = idx % subDim;
      if (parts[pidx].hasKey(subidx)) {
        resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), 1));
      }
    }

    return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
      resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVector[] parts = v1.getPartitions();
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        LongIntVector[] resParts = new LongIntVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongIntVector part = parts[i];
          resParts[i] =
            new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
        return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongIntVector resVect = v1.copy();
        LongIntVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongIntVector part : resParts) {
          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2IntMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        LongIntVector[] resParts = new LongIntVector[parts.length];
        for (int i = 0; i < parts.length; i++) {
          LongIntVector part = parts[i];
          resParts[i] =
            new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
              part.getStorage().emptySparse());
        }

        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }

        return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
          resParts, v1.getSubDim());
      } else {
        CompLongIntVector resVect = v1.copy();
        LongIntVector[] resParts = resVect.getPartitions();
        long base = 0;
        for (LongIntVector part : resParts) {
          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partValues.length; i++) {
              if (v2.hasKey(i + base)) {
                partValues[i] = op.apply(partValues[i], v2.get(i + base));
              } else {
                partValues[i] = 0;
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2IntMap.Entry entry = piter.next();
              long idx = entry.getLongKey() + base;
              if (v2.hasKey(idx)) {
                entry.setValue(op.apply(entry.getIntValue(), v2.get(idx)));
              } else {
                piter.remove();
              }
            }
          } else { // sorted
            long[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int i = 0; i < partIndices.length; i++) {
              long idx = partIndices[i] + base;
              if (v2.hasKey(idx)) {
                partValues[i] = op.apply(partValues[i], v2.get(idx));
              } else {
                partValues[i] = 0;
              }
            }
          }
          base += part.getDim();
        }
        return resVect;
      }
    }
  }


}