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
import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.*;
import it.unimi.dsi.fastutil.ints.*;
import it.unimi.dsi.fastutil.longs.*;
import com.tencent.angel.ml.math2.utils.Constant;
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
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);

	int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  int subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntDoubleVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  double[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntDoubleVector part : parts) {
		double[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  double[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2DoubleMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntDoubleVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntFloatVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  float[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntDoubleVector part : parts) {
		double[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  double[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2DoubleMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntDoubleVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntLongVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  long[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntDoubleVector part : parts) {
		double[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  double[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2DoubleMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntDoubleVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntDoubleVector v1, IntIntVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntDoubleVector part : parts) {
		double[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  double[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2DoubleMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2DoubleMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getDoubleValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntDoubleVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntFloatVector v1, IntDummyVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);

	int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  int subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntFloatVector v1, IntFloatVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  float[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntFloatVector part : parts) {
		float[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  float[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2FloatMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntFloatVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntFloatVector v1, IntLongVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  long[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntFloatVector part : parts) {
		float[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  float[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2FloatMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntFloatVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntFloatVector v1, IntIntVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntFloatVector part : parts) {
		float[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  float[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2FloatMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2FloatMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getFloatValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntFloatVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntLongVector v1, IntDummyVector v2, Binary op) {
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = StorageSwitch.apply(v1, v2, op);

	int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  int subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntLongVector v1, IntLongVector v2, Binary op) {
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  long[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntLongVector part : parts) {
		long[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  long[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2LongMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getLongValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntLongVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntLongVector v1, IntIntVector v2, Binary op) {
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntLongVector part : parts) {
		long[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  long[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2LongMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2LongMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getLongValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntLongVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompIntIntVector v1, IntDummyVector v2, Binary op) {
	IntIntVector[] parts = v1.getPartitions();
	IntIntVector[] resParts = StorageSwitch.apply(v1, v2, op);

	int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  int subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompIntIntVector v1, IntIntVector v2, Binary op) {
	IntIntVector[] parts = v1.getPartitions();
	IntIntVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int[] v2Values = v2.getStorage().getValues();
	  int base = 0, k = 0;
	  for (IntIntVector part : parts) {
		int[] newValues = resParts[k].getStorage().getValues();

		if (part.isDense()) {
		  int[] partValue = part.getStorage().getValues();
		  for (int i = 0; i < partValue.length; i++) {
			int idx = i + base;
			newValues[i] = op.apply(partValue[i], v2Values[idx]);
		  }
		} else if (part.isSparse()) {
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}
			ObjectIterator<Int2IntMap.Entry> iter = part.getStorage().entryIterator();
			while (iter.hasNext()) {
			  Int2IntMap.Entry entry = iter.next();
			  int idx = entry.getIntKey();
			  newValues[idx] = op.apply(entry.getIntValue(), v2Values[idx + base]);
			}
		  } else {
			for (int i = 0; i < newValues.length; i++) {
			  if (part.getStorage().hasKey(i)) {
				newValues[i] = op.apply(part.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		} else { // sorted
		  if (part.size() < Constant.denseLoopThreshold * part.getDim()) {
			int[] partIndices = part.getStorage().getIndices();
			int[] partValues = part.getStorage().getValues();
			for (int i = 0; i < part.getDim(); i++) {
			  newValues[i] = op.apply(0, v2Values[i + base]);
			}

			int size = part.size();
			for (int i = 0; i < size; i++) {
			  int idx = partIndices[i];
			  newValues[idx] = op.apply(partValues[i], v2Values[idx + base]);
			}
		  } else {
			IntIntVectorStorage partStorage = part.getStorage();
			for (int i = 0; i < newValues.length; i++) {
			  if (partStorage.hasKey(i)) {
				newValues[i] = op.apply(partStorage.get(i), v2Values[i + base]);
			  } else {
				newValues[i] = op.apply(0, v2Values[i + base]);
			  }
			}
		  }
		}

		base += part.getDim();
		k++;
	  }
	} else {
	  int subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	  for (int i = 0; i < v1.getDim(); i++) {
		int pidx = (int) (i / subDim);
		int subidx = i % subDim;

		if (v2.getStorage().hasKey(i)) {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
		} else {
		  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
		}
	  }
	}

	return new CompIntIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongDoubleVector v1, LongDummyVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);

	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongDoubleVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongFloatVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongLongVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongDoubleVector v1, LongIntVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongDoubleVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongFloatVector v1, LongDummyVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);

	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongFloatVector v1, LongFloatVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongFloatVector v1, LongLongVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongFloatVector v1, LongIntVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongFloatVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongLongVector v1, LongDummyVector v2, Binary op) {
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = StorageSwitch.apply(v1, v2, op);

	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongLongVector v1, LongLongVector v2, Binary op) {
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongLongVector v1, LongIntVector v2, Binary op) {
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongLongVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }


  private static Vector apply(CompLongIntVector v1, LongDummyVector v2, Binary op) {
	LongIntVector[] parts = v1.getPartitions();
	LongIntVector[] resParts = StorageSwitch.apply(v1, v2, op);

	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;
	  resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	}

	return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

  private static Vector apply(CompLongIntVector v1, LongIntVector v2, Binary op) {
	LongIntVector[] parts = v1.getPartitions();
	LongIntVector[] resParts = StorageSwitch.apply(v1, v2, op);
	long subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();

	for (int i = 0; i < v1.getDim(); i++) {
	  int pidx = (int) (i / subDim);
	  long subidx = i % subDim;

	  if (v2.getStorage().hasKey(i)) {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), v2.get(i)));
	  } else {
		resParts[pidx].set(subidx, op.apply(resParts[pidx].get(subidx), 0));
	  }
	}

	return new CompLongIntVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), resParts, v1.getSubDim());
  }

}