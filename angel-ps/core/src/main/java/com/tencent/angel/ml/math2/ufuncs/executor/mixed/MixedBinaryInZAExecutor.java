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
import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.storage.Storage;
import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
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
import com.tencent.angel.ml.math2.vector.IntDummyVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.vector.IntIntVector;
import com.tencent.angel.ml.math2.vector.IntLongVector;
import com.tencent.angel.ml.math2.vector.LongDoubleVector;
import com.tencent.angel.ml.math2.vector.LongDummyVector;
import com.tencent.angel.ml.math2.vector.LongFloatVector;
import com.tencent.angel.ml.math2.vector.LongIntVector;
import com.tencent.angel.ml.math2.vector.LongLongVector;
import com.tencent.angel.ml.math2.vector.Vector;
import it.unimi.dsi.fastutil.ints.Int2DoubleMap;
import it.unimi.dsi.fastutil.ints.Int2FloatMap;
import it.unimi.dsi.fastutil.ints.Int2IntMap;
import it.unimi.dsi.fastutil.ints.Int2LongMap;
import it.unimi.dsi.fastutil.longs.Long2DoubleMap;
import it.unimi.dsi.fastutil.longs.Long2FloatMap;
import it.unimi.dsi.fastutil.longs.Long2IntMap;
import it.unimi.dsi.fastutil.longs.Long2LongMap;
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class MixedBinaryInZAExecutor {

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
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      int[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        int subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      int base = 0;
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];

        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          double[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Int2DoubleMap.Entry entry = piter.next();
            int idx = entry.getIntKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    IntDoubleVector[] res = new IntDoubleVector[parts.length];
    int i = 0;
    for (IntDoubleVector part : parts) {
      res[i] = new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntDoubleVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      double[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
        if (part.isDense()) {
          double[] resPartValues = resPart.getValues();
          double[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2DoubleMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getDoubleValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntDoubleVector[] res = new IntDoubleVector[parts.length];
    int i = 0;
    for (IntDoubleVector part : parts) {
      res[i] = new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
        if (part.isDense()) {
          double[] resPartValues = resPart.getValues();
          double[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntDoubleVector[] res = new IntDoubleVector[parts.length];
    int i = 0;
    for (IntDoubleVector part : parts) {
      res[i] = new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
        if (part.isDense()) {
          double[] resPartValues = resPart.getValues();
          double[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntDoubleVector[] res = new IntDoubleVector[parts.length];
    int i = 0;
    for (IntDoubleVector part : parts) {
      res[i] = new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntDoubleVector part = parts[i];
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
        if (part.isDense()) {
          double[] resPartValues = resPart.getValues();
          double[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2DoubleMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntDoubleVector part = parts[i];
          IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2DoubleMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntDoubleVector[] res = new IntDoubleVector[parts.length];
    int i = 0;
    for (IntDoubleVector part : parts) {
      res[i] = new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompIntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      int[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        int subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      int base = 0;
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];

        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          float[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Int2FloatMap.Entry entry = piter.next();
            int idx = entry.getIntKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    IntFloatVector[] res = new IntFloatVector[parts.length];
    int i = 0;
    for (IntFloatVector part : parts) {
      res[i] = new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      float[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];
        if (part.isDense()) {
          float[] resPartValues = resPart.getValues();
          float[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getFloatValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2FloatMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];

          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntFloatVector[] res = new IntFloatVector[parts.length];
    int i = 0;
    for (IntFloatVector part : parts) {
      res[i] = new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];
        if (part.isDense()) {
          float[] resPartValues = resPart.getValues();
          float[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getFloatValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];

          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntFloatVector[] res = new IntFloatVector[parts.length];
    int i = 0;
    for (IntFloatVector part : parts) {
      res[i] = new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntFloatVector part = parts[i];
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];
        if (part.isDense()) {
          float[] resPartValues = resPart.getValues();
          float[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2FloatMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getFloatValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntFloatVector part = parts[i];
          IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[i];

          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2FloatMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntFloatVector[] res = new IntFloatVector[parts.length];
    int i = 0;
    for (IntFloatVector part : parts) {
      res[i] = new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompIntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      int[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        int subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((IntLongVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      int base = 0;
      for (int i = 0; i < parts.length; i++) {
        IntLongVector part = parts[i];
        IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];

        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          long[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Int2LongMap.Entry entry = piter.next();
            int idx = entry.getIntKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            int[] resPartIndices = resPart.getIndices();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    IntLongVector[] res = new IntLongVector[parts.length];
    int i = 0;
    for (IntLongVector part : parts) {
      res[i] = new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntLongVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      long[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntLongVector part = parts[i];
        IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];
        if (part.isDense()) {
          long[] resPartValues = resPart.getValues();
          long[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getLongValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            long[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2LongMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];

          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntLongVector[] res = new IntLongVector[parts.length];
    int i = 0;
    for (IntLongVector part : parts) {
      res[i] = new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntLongVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntLongVector part = parts[i];
        IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];
        if (part.isDense()) {
          long[] resPartValues = resPart.getValues();
          long[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2LongMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getLongValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            long[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntLongVector part = parts[i];
          IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[i];

          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2LongMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntLongVector[] res = new IntLongVector[parts.length];
    int i = 0;
    for (IntLongVector part : parts) {
      res[i] = new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntLongVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompIntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      int[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        int idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        int subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((IntIntVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      int base = 0;
      for (int i = 0; i < parts.length; i++) {
        IntIntVector part = parts[i];
        IntIntVectorStorage resPart = (IntIntVectorStorage) resParts[i];

        if (part.isDense()) {
          int[] partValues = part.getStorage().getValues();
          int[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2IntMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Int2IntMap.Entry entry = piter.next();
            int idx = entry.getIntKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getIntValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            int[] resPartIndices = resPart.getIndices();
            int[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    IntIntVector[] res = new IntIntVector[parts.length];
    int i = 0;
    for (IntIntVector part : parts) {
      res[i] = new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          (IntIntVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompIntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int base = 0;
      int[] v2Values = v2.getStorage().getValues();
      for (int i = 0; i < parts.length; i++) {
        IntIntVector part = parts[i];
        IntIntVectorStorage resPart = (IntIntVectorStorage) resParts[i];
        if (part.isDense()) {
          int[] resPartValues = resPart.getValues();
          int[] partValues = part.getStorage().getValues();
          for (int j = 0; j < partValues.length; j++) {
            resPartValues[j] = op.apply(partValues[j], v2Values[base + j]);
          }
        } else if (part.isSparse()) {
          ObjectIterator<Int2IntMap.Entry> iter = part.getStorage().entryIterator();
          while (iter.hasNext()) {
            Int2IntMap.Entry entry = iter.next();
            int idx = entry.getIntKey();
            resPart.set(idx, op.apply(entry.getIntValue(), v2Values[idx + base]));
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            int[] resPartValues = resPart.getValues();
            int[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPartIndices[j] = idx;
              resPartValues[j] = op.apply(partValues[j], v2Values[idx + base]);
            }
          } else {
            int[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              int idx = partIndices[j];
              resPart.set(idx, op.apply(partValues[j], v2Values[idx + base]));
            }
          }
        }
        base += part.getDim();
      }
    } else if (v2.isSparse()) {
      ObjectIterator<Int2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Int2IntMap.Entry entry = iter.next();
          int idx = entry.getIntKey();
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntIntVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntIntVector part = parts[i];
          IntIntVectorStorage resPart = (IntIntVectorStorage) resParts[i];
          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            int[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2IntMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getIntValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              int[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        int[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          int idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          int subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((IntIntVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        int base = 0;
        for (int i = 0; i < parts.length; i++) {
          IntIntVector part = parts[i];
          IntIntVectorStorage resPart = (IntIntVectorStorage) resParts[i];

          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            int[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Int2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Int2IntMap.Entry entry = piter.next();
              int idx = entry.getIntKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getIntValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              int[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              int[] resPartIndices = resPart.getIndices();
              int[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              int[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                int idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    IntIntVector[] res = new IntIntVector[parts.length];
    int i = 0;
    for (IntIntVector part : parts) {
      res[i] = new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          (IntIntVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompLongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      long[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        long subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((LongDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      long base = 0;
      for (int i = 0; i < parts.length; i++) {
        LongDoubleVector part = parts[i];
        LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];

        if (part.isDense()) {
          double[] partValues = part.getStorage().getValues();
          double[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Long2DoubleMap.Entry entry = piter.next();
            long idx = entry.getLongKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            long[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            long[] partIndices = part.getStorage().getIndices();
            double[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2DoubleMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2DoubleMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getDoubleValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        double[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];
          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongDoubleVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongDoubleVector part = parts[i];
          LongDoubleVectorStorage resPart = (LongDoubleVectorStorage) resParts[i];

          if (part.isDense()) {
            double[] partValues = part.getStorage().getValues();
            double[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2DoubleMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2DoubleMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getDoubleValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              double[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompLongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      long[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        long subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((LongFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      long base = 0;
      for (int i = 0; i < parts.length; i++) {
        LongFloatVector part = parts[i];
        LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];

        if (part.isDense()) {
          float[] partValues = part.getStorage().getValues();
          float[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Long2FloatMap.Entry entry = piter.next();
            long idx = entry.getLongKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            long[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            long[] partIndices = part.getStorage().getIndices();
            float[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2FloatMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2FloatMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        float[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];

          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];

          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];
          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongFloatVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongFloatVector part = parts[i];
          LongFloatVectorStorage resPart = (LongFloatVectorStorage) resParts[i];

          if (part.isDense()) {
            float[] partValues = part.getStorage().getValues();
            float[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2FloatMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2FloatMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getFloatValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              float[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompLongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      long[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        long subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((LongLongVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      long base = 0;
      for (int i = 0; i < parts.length; i++) {
        LongLongVector part = parts[i];
        LongLongVectorStorage resPart = (LongLongVectorStorage) resParts[i];

        if (part.isDense()) {
          long[] partValues = part.getStorage().getValues();
          long[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Long2LongMap.Entry entry = piter.next();
            long idx = entry.getLongKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            long[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            long[] resPartIndices = resPart.getIndices();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            long[] partIndices = part.getStorage().getIndices();
            long[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    LongLongVector[] res = new LongLongVector[parts.length];
    int i = 0;
    for (LongLongVector part : parts) {
      res[i] = new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongLongVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2LongMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2LongMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          LongLongVectorStorage resPart = (LongLongVectorStorage) resParts[i];
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        long[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          LongLongVectorStorage resPart = (LongLongVectorStorage) resParts[i];

          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongLongVector[] res = new LongLongVector[parts.length];
    int i = 0;
    for (LongLongVector part : parts) {
      res[i] = new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongLongVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          LongLongVectorStorage resPart = (LongLongVectorStorage) resParts[i];
          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongLongVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongLongVector part = parts[i];
          LongLongVectorStorage resPart = (LongLongVectorStorage) resParts[i];

          if (part.isDense()) {
            long[] partValues = part.getStorage().getValues();
            long[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2LongMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2LongMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getLongValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              long[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongLongVector[] res = new LongLongVector[parts.length];
    int i = 0;
    for (LongLongVector part : parts) {
      res[i] = new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongLongVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


  private static Vector apply(CompLongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (v1.size() > v2.size()) {
      long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
      long[] v2Indices = v2.getIndices();
      for (int i = 0; i < v2Indices.length; i++) {
        long idx = v2Indices[i];
        int pidx = (int) (idx / subDim);
        long subidx = idx % subDim;
        if (parts[pidx].hasKey(subidx)) {
          ((LongIntVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 1));
        }
      }
    } else {
      long base = 0;
      for (int i = 0; i < parts.length; i++) {
        LongIntVector part = parts[i];
        LongIntVectorStorage resPart = (LongIntVectorStorage) resParts[i];

        if (part.isDense()) {
          int[] partValues = part.getStorage().getValues();
          int[] resPartValues = resPart.getValues();
          for (int j = 0; j < partValues.length; j++) {
            if (v2.hasKey(j + base)) {
              resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
            }
          }
        } else if (part.isSparse()) {
          ObjectIterator<Long2IntMap.Entry> piter = part.getStorage().entryIterator();
          while (piter.hasNext()) {
            Long2IntMap.Entry entry = piter.next();
            long idx = entry.getLongKey();
            if (v2.hasKey(idx + base)) {
              resPart.set(idx, op.apply(entry.getIntValue(), v2.get(idx + base)));
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            long[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            long[] resPartIndices = resPart.getIndices();
            int[] resPartValues = resPart.getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPartIndices[j] = idx;
                resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
              }
            }
          } else {
            long[] partIndices = part.getStorage().getIndices();
            int[] partValues = part.getStorage().getValues();
            for (int j = 0; j < partIndices.length; j++) {
              long idx = partIndices[j];
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
              }
            }
          }
        }
        base += part.getDim();
      }
    }
    LongIntVector[] res = new LongIntVector[parts.length];
    int i = 0;
    for (LongIntVector part : parts) {
      res[i] = new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongIntVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }

  private static Vector apply(CompLongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isSparse()) {
      ObjectIterator<Long2IntMap.Entry> iter = v2.getStorage().entryIterator();
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        while (iter.hasNext()) {
          Long2IntMap.Entry entry = iter.next();
          long idx = entry.getLongKey();
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongIntVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongIntVector part = parts[i];
          LongIntVectorStorage resPart = (LongIntVectorStorage) resParts[i];
          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            int[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2IntMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getIntValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              int[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }
          base += part.getDim();
        }
      }
    } else { // sorted
      if (v1.size() > v2.size()) {
        long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();
        long[] v2Indices = v2.getStorage().getIndices();
        int[] v2Values = v2.getStorage().getValues();
        for (int i = 0; i < v2Indices.length; i++) {
          long idx = v2Indices[i];
          int pidx = (int) (idx / subDim);
          long subidx = idx % subDim;
          if (parts[pidx].hasKey(subidx)) {
            ((LongIntVectorStorage) resParts[pidx])
                .set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
          }
        }
      } else {
        long base = 0;
        for (int i = 0; i < parts.length; i++) {
          LongIntVector part = parts[i];
          LongIntVectorStorage resPart = (LongIntVectorStorage) resParts[i];

          if (part.isDense()) {
            int[] partValues = part.getStorage().getValues();
            int[] resPartValues = resPart.getValues();
            for (int j = 0; j < partValues.length; j++) {
              if (v2.hasKey(j + base)) {
                resPartValues[j] = op.apply(partValues[j], v2.get(j + base));
              }
            }
          } else if (part.isSparse()) {
            ObjectIterator<Long2IntMap.Entry> piter = part.getStorage().entryIterator();
            while (piter.hasNext()) {
              Long2IntMap.Entry entry = piter.next();
              long idx = entry.getLongKey();
              if (v2.hasKey(idx + base)) {
                resPart.set(idx, op.apply(entry.getIntValue(), v2.get(idx + base)));
              }
            }
          } else { // sorted
            if (op.isKeepStorage()) {
              long[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              long[] resPartIndices = resPart.getIndices();
              int[] resPartValues = resPart.getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPartIndices[j] = idx;
                  resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));
                }
              }
            } else {
              long[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();
              for (int j = 0; j < partIndices.length; j++) {
                long idx = partIndices[j];
                if (v2.hasKey(idx + base)) {
                  resPart.set(idx, op.apply(partValues[j], v2.get(idx + base)));
                }
              }
            }
          }

          base += part.getDim();
        }
      }
    }
    LongIntVector[] res = new LongIntVector[parts.length];
    int i = 0;
    for (LongIntVector part : parts) {
      res[i] = new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongIntVectorStorage) resParts[i]);
      i++;
    }
    v1.setPartitions(res);

    return v1;
  }


}