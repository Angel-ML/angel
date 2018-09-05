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
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntDoubleVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  double[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntDoubleVector part = parts[i];
		double[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  double[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2DoubleMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2DoubleMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  double[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getDoubleValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntFloatVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  float[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntDoubleVector part = parts[i];
		double[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  double[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2DoubleMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2DoubleMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  double[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntLongVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  long[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntDoubleVector part = parts[i];
		double[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  double[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2DoubleMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2DoubleMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  double[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntDoubleVector v1, IntIntVector v2, Binary op) {
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  int[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntDoubleVector part = parts[i];
		double[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  double[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2DoubleMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2DoubleMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getDoubleValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  double[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompIntFloatVector v1, IntDummyVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntFloatVector v1, IntFloatVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  float[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntFloatVector part = parts[i];
		float[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  float[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2FloatMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2FloatMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  float[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntFloatVector v1, IntLongVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  long[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntFloatVector part = parts[i];
		float[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  float[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2FloatMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2FloatMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  float[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntFloatVector v1, IntIntVector v2, Binary op) {
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  int[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntFloatVector part = parts[i];
		float[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  float[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2FloatMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2FloatMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getFloatValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  float[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompIntLongVector v1, IntDummyVector v2, Binary op) {
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntLongVector v1, IntLongVector v2, Binary op) {
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  long[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntLongVector part = parts[i];
		long[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  long[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2LongMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2LongMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  long[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntLongVector v1, IntIntVector v2, Binary op) {
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  int[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntLongVector part = parts[i];
		long[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  long[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2LongMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2LongMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getLongValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  long[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompIntIntVector v1, IntDummyVector v2, Binary op) {
	IntIntVector[] parts = v1.getPartitions();
	IntIntVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompIntIntVector v1, IntIntVector v2, Binary op) {
	IntIntVector[] parts = v1.getPartitions();
	IntIntVector[] resParts = StorageSwitch.apply(v1, v2, op);
	if (v2.isDense()) {
	  int base = 0;
	  int[] v2Values = v2.getStorage().getValues();
	  for (int i = 0; i < parts.length; i++) {
		IntIntVector part = parts[i];
		int[] newValues = resParts[i].getStorage().getValues();
		if (part.isDense()) {
		  int[] artValues = part.getStorage().getValues();
		  for (int j = 0; j < artValues.length; j++) {
			newValues[j] = op.apply(artValues[j], v2Values[base + j]);
		  }
		} else if (part.isSparse()) {
		  ObjectIterator<Int2IntMap.Entry> iter = resParts[i].getStorage().entryIterator();
		  while (iter.hasNext()) {
			Int2IntMap.Entry entry = iter.next();
			int idx = entry.getIntKey() + base;
			entry.setValue(op.apply(entry.getIntValue(), v2Values[idx]));
		  }
		} else { // sorted
		  int[] partIndices = part.getStorage().getIndices();
		  int[] partValues = part.getStorage().getValues();
		  for (int j = 0; j < partIndices.length; j++) {
			int idx = partIndices[j] + base;
			newValues[j] = op.apply(partValues[j], v2Values[idx]);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntIntVector part : parts) {
		  int[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			int[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2IntMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		int base = 0;
		int k = 0;
		for (IntIntVector part : parts) {
		  int[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			int[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Int2IntMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompLongDoubleVector v1, LongDummyVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongDoubleVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getDoubleValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongFloatVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongLongVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongDoubleVector v1, LongIntVector v2, Binary op) {
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongDoubleVector part : parts) {
		  double[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			double[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2DoubleMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompLongFloatVector v1, LongDummyVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongFloatVector v1, LongFloatVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getFloatValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongFloatVector v1, LongLongVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongFloatVector v1, LongIntVector v2, Binary op) {
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongFloatVector part : parts) {
		  float[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			float[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2FloatMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompLongLongVector v1, LongDummyVector v2, Binary op) {
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongLongVector v1, LongLongVector v2, Binary op) {
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getLongValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongLongVector v1, LongIntVector v2, Binary op) {
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongLongVector part : parts) {
		  long[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			long[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2LongMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


  private static Vector apply(CompLongIntVector v1, LongDummyVector v2, Binary op) {
	LongIntVector[] parts = v1.getPartitions();
	LongIntVector[] resParts = StorageSwitch.apply(v1, v2, op);

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
	v1.setPartitions(resParts);

	return v1;
  }

  private static Vector apply(CompLongIntVector v1, LongIntVector v2, Binary op) {
	LongIntVector[] parts = v1.getPartitions();
	LongIntVector[] resParts = StorageSwitch.apply(v1, v2, op);
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), entry.getIntValue()));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongIntVector part : parts) {
		  int[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			int[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2IntMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }
		  base += part.getDim();
		  k++;
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
			resParts[pidx].set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));
		  }
		}
	  } else {
		long base = 0;
		int k = 0;
		for (LongIntVector part : parts) {
		  int[] newValues = resParts[k].getStorage().getValues();
		  if (part.isDense()) {
			int[] partValues = part.getStorage().getValues();
			for (int i = 0; i < partValues.length; i++) {
			  if (v2.hasKey(i + base)) {
				newValues[i] = op.apply(partValues[i], v2.get(i + base));
			  } else {
				newValues[i] = 0;
			  }
			}
		  } else if (part.isSparse()) {
			ObjectIterator<Long2IntMap.Entry> piter = resParts[k].getStorage().entryIterator();
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
				newValues[i] = op.apply(partValues[i], v2.get(idx));
			  } else {
				newValues[i] = 0;
			  }
			}
		  }

		  base += part.getDim();
		  k++;
		}
	  }
	}
	v1.setPartitions(resParts);

	return v1;
  }


}