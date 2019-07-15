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
import com.tencent.angel.ml.math2.storage.IntDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntIntVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.IntLongVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongDoubleVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongFloatVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongIntVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSortedVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongSparseVectorStorage;
import com.tencent.angel.ml.math2.storage.LongLongVectorStorage;
import com.tencent.angel.ml.math2.storage.Storage;
import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.utils.Constant;
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
import it.unimi.dsi.fastutil.objects.ObjectIterator;

public class MixedBinaryOutAllExecutor {

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

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof IntDoubleSortedVectorStorage) {
          resParts[i] = new IntDoubleSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      int subidx = i % subDim;
      ((IntDoubleVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    IntDoubleVector[] res = new IntDoubleVector[parts.length];
    int i = 0;
    for (IntDoubleVector part : parts) {
      res[i] = new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntDoubleVectorStorage) resParts[i]);
      i++;
    }
    return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntDoubleVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      double[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntDoubleVector part : parts) {
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[k];

        if (part.isDense()) {
          double[] partValue = part.getStorage().getValues();
          double[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2DoubleMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntDoubleSortedVectorStorage) {
            resParts[i] = new IntDoubleSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntFloatVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      float[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntDoubleVector part : parts) {
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[k];

        if (part.isDense()) {
          double[] partValue = part.getStorage().getValues();
          double[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2DoubleMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntDoubleSortedVectorStorage) {
            resParts[i] = new IntDoubleSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntLongVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      long[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntDoubleVector part : parts) {
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[k];

        if (part.isDense()) {
          double[] partValue = part.getStorage().getValues();
          double[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2DoubleMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntDoubleSortedVectorStorage) {
            resParts[i] = new IntDoubleSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntIntVector v2, Binary op) {
    IntDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntDoubleVector part : parts) {
        IntDoubleVectorStorage resPart = (IntDoubleVectorStorage) resParts[k];

        if (part.isDense()) {
          double[] partValue = part.getStorage().getValues();
          double[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2DoubleMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getDoubleValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            double[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              double[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntDoubleVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntDoubleSortedVectorStorage) {
            resParts[i] = new IntDoubleSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntDoubleVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }


  private static Vector apply(CompIntFloatVector v1, IntDummyVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof IntFloatSortedVectorStorage) {
          resParts[i] = new IntFloatSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      int subidx = i % subDim;
      ((IntFloatVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    IntFloatVector[] res = new IntFloatVector[parts.length];
    int i = 0;
    for (IntFloatVector part : parts) {
      res[i] = new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntFloatVectorStorage) resParts[i]);
      i++;
    }
    return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntFloatVector v1, IntFloatVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      float[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntFloatVector part : parts) {
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[k];

        if (part.isDense()) {
          float[] partValue = part.getStorage().getValues();
          float[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2FloatMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getFloatValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntFloatVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            float[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntFloatVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntFloatSortedVectorStorage) {
            resParts[i] = new IntFloatSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntFloatVector v1, IntLongVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      long[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntFloatVector part : parts) {
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[k];

        if (part.isDense()) {
          float[] partValue = part.getStorage().getValues();
          float[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2FloatMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getFloatValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntFloatVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            float[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntFloatVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntFloatSortedVectorStorage) {
            resParts[i] = new IntFloatSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntFloatVector v1, IntIntVector v2, Binary op) {
    IntFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntFloatVector part : parts) {
        IntFloatVectorStorage resPart = (IntFloatVectorStorage) resParts[k];

        if (part.isDense()) {
          float[] partValue = part.getStorage().getValues();
          float[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2FloatMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getFloatValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            float[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntFloatVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            float[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              float[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntFloatVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntFloatSortedVectorStorage) {
            resParts[i] = new IntFloatSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntFloatVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }


  private static Vector apply(CompIntLongVector v1, IntDummyVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof IntLongSortedVectorStorage) {
          resParts[i] = new IntLongSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      int subidx = i % subDim;
      ((IntLongVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    IntLongVector[] res = new IntLongVector[parts.length];
    int i = 0;
    for (IntLongVector part : parts) {
      res[i] = new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (IntLongVectorStorage) resParts[i]);
      i++;
    }
    return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntLongVector v1, IntLongVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      long[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntLongVector part : parts) {
        IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[k];

        if (part.isDense()) {
          long[] partValue = part.getStorage().getValues();
          long[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2LongMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getLongValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            long[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntLongVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            long[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntLongVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntLongSortedVectorStorage) {
            resParts[i] = new IntLongSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntLongVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntLongVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntLongVector v1, IntIntVector v2, Binary op) {
    IntLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntLongVector part : parts) {
        IntLongVectorStorage resPart = (IntLongVectorStorage) resParts[k];

        if (part.isDense()) {
          long[] partValue = part.getStorage().getValues();
          long[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2LongMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getLongValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            long[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntLongVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            long[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              long[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntLongVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntLongSortedVectorStorage) {
            resParts[i] = new IntLongSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntLongVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntLongVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }


  private static Vector apply(CompIntIntVector v1, IntDummyVector v2, Binary op) {
    IntIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof IntIntSortedVectorStorage) {
          resParts[i] = new IntIntSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      int subidx = i % subDim;
      ((IntIntVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    IntIntVector[] res = new IntIntVector[parts.length];
    int i = 0;
    for (IntIntVector part : parts) {
      res[i] = new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
          (IntIntVectorStorage) resParts[i]);
      i++;
    }
    return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompIntIntVector v1, IntIntVector v2, Binary op) {
    IntIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (v2.isDense()) {
      int[] v2Values = v2.getStorage().getValues();
      int base = 0, k = 0;
      for (IntIntVector part : parts) {
        IntIntVectorStorage resPart = (IntIntVectorStorage) resParts[k];

        if (part.isDense()) {
          int[] partValue = part.getStorage().getValues();
          int[] resPartValues = resPart.getValues();
          for (int i = 0; i < partValue.length; i++) {
            int idx = i;
            resPartValues[i] = op.apply(partValue[i], v2Values[idx + base]);
          }
        } else if (part.isSparse()) {
          if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
            for (int i = 0; i < part.getDim(); i++) {
              resPart.set(i, op.apply(0, v2Values[i + base]));
            }
            ObjectIterator<Int2IntMap.Entry> iter = part.getStorage().entryIterator();
            while (iter.hasNext()) {
              Int2IntMap.Entry entry = iter.next();
              int idx = entry.getIntKey();
              resPart.set(idx, op.apply(entry.getIntValue(), v2Values[idx + base]));
            }
          } else {
            for (int i = 0; i < resPart.size(); i++) {
              if (part.getStorage().hasKey(i)) {
                resPart.set(i, op.apply(part.get(i), v2Values[i + base]));
              } else {
                resPart.set(i, op.apply(0, v2Values[i + base]));
              }
            }
          }
        } else { // sorted
          if (op.isKeepStorage()) {
            int[] resPartIndices = resPart.getIndices();
            int[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartIndices[i] = i;
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntIntVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartIndices[i] = i;
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          } else {
            int[] resPartValues = resPart.getValues();
            if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
              int[] partIndices = part.getStorage().getIndices();
              int[] partValues = part.getStorage().getValues();

              for (int i = 0; i < part.getDim(); i++) {
                resPartValues[i] = op.apply(0, v2Values[i + base]);
              }

              int size = part.size();
              for (int i = 0; i < size; i++) {
                int idx = partIndices[i];
                resPartValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
              }
            } else {
              IntIntVectorStorage partStorage = part.getStorage();
              for (int i = 0; i < resPartValues.length; i++) {
                if (partStorage.hasKey(i)) {
                  resPartValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
                } else {
                  resPartValues[i] = op.apply(0, v2Values[i + base]);
                }
              }
            }
          }
        }

        base += part.getDim();
        k++;
      }
    } else {
      if (!op.isKeepStorage()) {
        for (int i = 0; i < parts.length; i++) {
          if (parts[i].getStorage() instanceof IntIntSortedVectorStorage) {
            resParts[i] = new IntIntSparseVectorStorage(parts[i].getDim(),
                parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
          }
        }
      }

      int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

      for (int i = 0; i < v1.getDim(); i++) {
        int pidx = (int) (i / subDim);
        int subidx = i % subDim;

        if (v2.getStorage().hasKey(i)) {
          ((IntIntVectorStorage) resParts[pidx])
              .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
        } else {
          ((IntIntVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
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

    return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }


  private static Vector apply(CompLongDoubleVector v1, LongDummyVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongDoubleSortedVectorStorage) {
          resParts[i] = new LongDoubleSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;
      ((LongDoubleVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }
    return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        res, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongDoubleVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongDoubleSortedVectorStorage) {
          resParts[i] = new LongDoubleSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        res, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongFloatVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongDoubleSortedVectorStorage) {
          resParts[i] = new LongDoubleSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        res, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongLongVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongDoubleSortedVectorStorage) {
          resParts[i] = new LongDoubleSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        res, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongIntVector v2, Binary op) {
    LongDoubleVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongDoubleSortedVectorStorage) {
          resParts[i] = new LongDoubleSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongDoubleVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongDoubleVector[] res = new LongDoubleVector[parts.length];
    int i = 0;
    for (LongDoubleVector part : parts) {
      res[i] = new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongDoubleVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),
        res, v1.getSubDim());
  }


  private static Vector apply(CompLongFloatVector v1, LongDummyVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongFloatSortedVectorStorage) {
          resParts[i] = new LongFloatSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;
      ((LongFloatVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }
    return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompLongFloatVector v1, LongFloatVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongFloatSortedVectorStorage) {
          resParts[i] = new LongFloatSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongFloatVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongFloatVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompLongFloatVector v1, LongLongVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongFloatSortedVectorStorage) {
          resParts[i] = new LongFloatSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongFloatVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongFloatVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompLongFloatVector v1, LongIntVector v2, Binary op) {
    LongFloatVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongFloatSortedVectorStorage) {
          resParts[i] = new LongFloatSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongFloatVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongFloatVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongFloatVector[] res = new LongFloatVector[parts.length];
    int i = 0;
    for (LongFloatVector part : parts) {
      res[i] = new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongFloatVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }


  private static Vector apply(CompLongLongVector v1, LongDummyVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongLongSortedVectorStorage) {
          resParts[i] = new LongLongSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;
      ((LongLongVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    LongLongVector[] res = new LongLongVector[parts.length];
    int i = 0;
    for (LongLongVector part : parts) {
      res[i] = new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongLongVectorStorage) resParts[i]);
      i++;
    }
    return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompLongLongVector v1, LongLongVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongLongSortedVectorStorage) {
          resParts[i] = new LongLongSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongLongVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongLongVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongLongVector[] res = new LongLongVector[parts.length];
    int i = 0;
    for (LongLongVector part : parts) {
      res[i] = new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongLongVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompLongLongVector v1, LongIntVector v2, Binary op) {
    LongLongVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongLongSortedVectorStorage) {
          resParts[i] = new LongLongSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongLongVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongLongVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongLongVector[] res = new LongLongVector[parts.length];
    int i = 0;
    for (LongLongVector part : parts) {
      res[i] = new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongLongVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }


  private static Vector apply(CompLongIntVector v1, LongDummyVector v2, Binary op) {
    LongIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);

    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongIntSortedVectorStorage) {
          resParts[i] = new LongIntSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;
      ((LongIntVectorStorage) resParts[pidx])
          .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
    }

    LongIntVector[] res = new LongIntVector[parts.length];
    int i = 0;
    for (LongIntVector part : parts) {
      res[i] = new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongIntVectorStorage) resParts[i]);
      i++;
    }
    return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

  private static Vector apply(CompLongIntVector v1, LongIntVector v2, Binary op) {
    LongIntVector[] parts = v1.getPartitions();
    Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);
    if (!op.isKeepStorage()) {
      for (int i = 0; i < parts.length; i++) {
        if (parts[i].getStorage() instanceof LongIntSortedVectorStorage) {
          resParts[i] = new LongIntSparseVectorStorage(parts[i].getDim(),
              parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());
        }
      }
    }

    long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

    for (int i = 0; i < v1.getDim(); i++) {
      int pidx = (int) (i / subDim);
      long subidx = i % subDim;

      if (v2.getStorage().hasKey(i)) {
        ((LongIntVectorStorage) resParts[pidx])
            .set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));
      } else {
        ((LongIntVectorStorage) resParts[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 0));
      }
    }
    LongIntVector[] res = new LongIntVector[parts.length];
    int i = 0;
    for (LongIntVector part : parts) {
      res[i] = new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(),
          part.getDim(), (LongIntVectorStorage) resParts[i]);
      i++;
    }

    return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res,
        v1.getSubDim());
  }

}