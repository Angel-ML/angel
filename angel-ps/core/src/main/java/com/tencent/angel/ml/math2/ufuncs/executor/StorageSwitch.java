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


package com.tencent.angel.ml.math2.ufuncs.executor;

import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.ufuncs.expression.OpType;
import com.tencent.angel.ml.math2.utils.Constant;
import com.tencent.angel.ml.math2.vector.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

public class StorageSwitch {
  private static Storage emptyStorage(Storage target, StorageMethod method, long capacity) {
	try {
	  Method m = target.getClass().getDeclaredMethod(method.toString(), int.class);
	  return (Storage) m.invoke(target, (int) capacity);
	} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
	  e.printStackTrace();
	}

	return null;
  }

  private static Storage emptyStorage(Storage target, StorageMethod method) {
	try {
	  Method m = target.getClass().getDeclaredMethod(method.toString());
	  return (Storage) m.invoke(target);
	} catch (NoSuchMethodException | IllegalAccessException | InvocationTargetException e) {
	  e.printStackTrace();
	}

	return null;
  }

  private static long allocSize(long v1Size, long v2Size, long dim) {
	if (v1Size + v2Size > dim) {
	  return dim;
	} else {
	  return v1Size + v2Size;
	}
  }

  private static Storage union(Vector v1, Vector v2, Binary op) {
	if (v1.isDense()) {
	  // KeepStorage is guaranteed
	  if (op.isInplace()) {
		return v1.getStorage();
	  } else {
		return v1.copy().getStorage();
	  }
	} else if (v1.isSparse()) {
	  if (v2.isDense()) {
		// ignore the isInplace option, since v2 is dense
		if (op.isKeepStorage()) {
		  // the value in old storage can be changed safe, so switch a storage
		  // but user required keep storage, we can prevent rehash
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v1.dim());
		} else {
		  return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		}
	  } else {// v2.isSparse() || v2.isSorted()
		long v1Size = v1.getSize();
		long v2Size = v2.getSize();

		if (v1Size >= v2Size * Constant.sparseThreshold &&
				(v1Size + v2Size) * Constant.intersectionCoeff <= Constant.sparseDenseStorageThreshold * v1.dim()) {
		  // we gauss the indices of v2 maybe is a subset of v1, or overlap is very large
		  // KeepStorage is guaranteed
		  if (op.isInplace()) {
			return v1.getStorage();
		  } else {
			return v1.copy().getStorage();
		  }
		} else if ((v1Size + v2Size) * Constant.intersectionCoeff >= Constant.sparseDenseStorageThreshold * v1.dim()) {
		  // we gauss dense storage is more efficient
		  if (op.isKeepStorage() || v1.getStorage() instanceof LongKeyVectorStorage) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v1.dim());
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  }
		} else {
		  // v1Size < v2Size * Constant.sparseThreshold
		  int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1Size / 0.75)));
		  // KeepStorage is guaranteed
		  if (v1Size + v2Size <= 1.5 * capacity) {
			if (op.isInplace()) {
			  return v1.getStorage();
			} else {
			  return v1.copy().getStorage();
			}
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, allocSize(v1Size, v2Size, v1.dim()));
		  }
		}
	  }
	} else {// v1.isSorted()
	  if (v2.isDense()) {
		// ignore the isInplace option, since v2 is dense
		if (op.isKeepStorage()) {
		  // the value in old storage can be changed safe, so switch a storage
		  // but user required keep storage, we can prevent rehash
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySorted, v1.dim());
		} else {
		  return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		}
	  } else {// v2.isSparse() || v2.isSorted()
		long v1Size = v1.getSize();
		long v2Size = v2.getSize();

		if ((v1Size + v2Size) * Constant.intersectionCoeff >= Constant.sortedDenseStorageThreshold * v1.dim()) {
		  // we gauss dense storage is more efficient
		  if (op.isKeepStorage()) {
			// prevent rehash
			return emptyStorage(v1.getStorage(), StorageMethod.emptySorted, allocSize(v1Size, v2Size, v1.dim()));
		  } else if (v1.getStorage() instanceof LongKeyVectorStorage) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, allocSize(v1Size, v2Size, v1.dim()));
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  }
		} else {
		  if (op.isKeepStorage()) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySorted, allocSize(v1Size, v2Size, v1.dim()));
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, allocSize(v1Size, v2Size, v1.dim()));
		  }
		}
	  }
	}
  }

  private static Storage intersection(Vector v1, Vector v2, Binary op) {
	if (v1.isDense()) {
	  if (v2.isDense()) {
		// KeepStorage is guaranteed
		return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
	  } else {// v2.isSparse() || v2.isSorted()
		if ((!v2.isSorted() && v2.getSize() >= Constant.sparseDenseStorageThreshold * v2.dim()) ||
				(v2.isSorted() && v2.getSize() >= Constant.sortedDenseStorageThreshold * v2.dim())) {
		  // dense preferred, KeepStorage is guaranteed
		  return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		} else { // sparse preferred
		  if (op.isKeepStorage()) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v2.getSize());
		  }
		}
	  }
	} else if (v1.isSparse()) {
	  if (v2.isDense()) {
		if (op.isKeepStorage() || v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
		  // sparse preferred, keep storage guaranteed
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySparse);
		} else {
		  // dense preferred
		  return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		}
	  } else { // v2.isSparse() || v2.isSorted()
		if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sparseDenseStorageThreshold * v2.dim()) {
		  // sparse preferred, keep storage guaranteed
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v2.getSize());
		} else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sparseDenseStorageThreshold * v1.dim()) {
		  // sparse preferred, keep storage guaranteed
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySparse);
		} else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()) {
		  // preferred dense
		  if (op.isKeepStorage() || v1.getStorage() instanceof LongKeyVectorStorage) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v2.getSize());
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  }
		} else { // preferred dense
		  if (op.isKeepStorage() || v1.getStorage() instanceof LongKeyVectorStorage) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse);
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  }
		}
	  }
	} else {//v1.isSorted()
	  if (v2.isDense()) {
		if (op.isKeepStorage() || v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
		  // sorted preferred
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySorted);
		} else {
		  return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		}
	  } else { // v2.isSparse() || v2.isSorted()
		if (v1.getSize() >= v2.getSize() && v2.getSize() <= Constant.sortedDenseStorageThreshold * v2.dim()) {
		  if (op.isKeepStorage()) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySorted, v2.getSize());
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v2.getSize());
		  }
		} else if (v1.getSize() <= v2.getSize() && v1.getSize() <= Constant.sortedDenseStorageThreshold * v1.dim()) {
		  if (op.isKeepStorage()) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySorted);
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse);
		  }
		} else if (v1.getSize() > v2.getSize() && v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim()) {
		  if (op.isKeepStorage()) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySorted, v2.getSize());
		  } else if (v1.getStorage() instanceof LongKeyVectorStorage) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v2.getSize());
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  }
		} else {
		  if (op.isKeepStorage()) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySorted);
		  } else if (v1.getStorage() instanceof LongKeyVectorStorage) {
			return emptyStorage(v1.getStorage(), StorageMethod.emptySparse);
		  } else {
			return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
		  }
		}
	  }
	}
  }

  private static Storage all(Vector v1, Vector v2, Binary op) {
	if (v1.isDense()) {
	  if (op.isInplace()) {
		return v1.getStorage();
	  } else {
		return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
	  }
	} else {
	  if (op.isKeepStorage()) {
		if (v1.isSparse()) {
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v1.dim());
		} else { // sorted
		  return emptyStorage(v1.getStorage(), StorageMethod.emptySorted, v1.dim());
		}
	  } else if (v1.getStorage() instanceof LongKeyVectorStorage) {
		return emptyStorage(v1.getStorage(), StorageMethod.emptySparse, v1.dim());
	  } else {
		return emptyStorage(v1.getStorage(), StorageMethod.emptyDense);
	  }
	}
  }

  public static Storage apply(Vector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  return union(v1, v2, op);
	} else if (op.getOpType() == OpType.INTERSECTION) {
	  return intersection(v1, v2, op);
	} else {// OpType.ALL
	  return all(v1, v2, op);
	}
  }

  public static Storage[] applyComp(ComponentVector v1, Vector v2, Binary op) {
	Vector[] parts = v1.getPartitions();
	Storage[] resParts = new Storage[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION) {
	  if (v2.isDense()) {
		for (Vector part : parts) {
		  if (part.isDense()) {
			if (op.isInplace()) {
			  resParts[k] = part.getStorage();
			} else {
			  resParts[k] = part.copy().getStorage();
			}
		  } else if (part.isSparse()) {
			if (op.isKeepStorage()) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse, part.dim());
			} else {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
			}
		  } else {
			if (op.isKeepStorage()) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySorted, part.dim());
			} else {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
			}
		  }
		  k++;
		}
	  } else {
		for (Vector part : parts) {
		  if (op.isInplace()) {
			resParts[k] = part.getStorage();
		  } else {
			resParts[k] = part.copy().getStorage();
		  }
		  k++;
		}
	  }
	} else if (op.getOpType() == OpType.INTERSECTION) {
	  if (v2.isDense()) {
		for (Vector part : parts) {
		  if (part.isDense()) {
			resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
		  } else if (part.isSparse()) {
			resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse);
		  } else {
			if (op.isKeepStorage()) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySorted);
			} else {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse);
			}
		  }
		  k++;
		}
	  } else {
		if (((Vector)v1).getSize() > v2.getSize()) {
		  for (Vector part : parts) {
			if (op.isKeepStorage()) {
			  if (part.isDense()) {
				resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
			  } else if (part.isSparse()) {
				resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse);
			  } else {
				resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySorted);
			  }
			} else {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse);
			}
			k++;
		  }
		} else {
		  for (Vector part : parts) {
			if (part.isDense()) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
			} else if (part.isSparse()) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse);
			} else {
			  if (op.isKeepStorage()) {
				resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySorted);
			  } else {
				resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse);
			  }
			}
			k++;
		  }
		}
	  }
	} else {//OpType.ALL
	  for (Vector part : parts) {
		if (part.isDense()) {
		  if (op.isInplace()) {
			resParts[k] = part.getStorage();
		  } else {
			resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
		  }
		} else {
		  if (op.isKeepStorage()) {
			if (part.isSparse()) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse, part.dim());
			} else { // sorted
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySorted, part.dim());
			}
		  } else {
			if (part.getStorage() instanceof LongKeyVectorStorage) {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptySparse, part.dim());
			} else {
			  resParts[k] = emptyStorage(part.getStorage(), StorageMethod.emptyDense);
			}
		  }
		}
		k++;
	  }
	}
	return resParts;
  }

  private enum StorageMethod {
	emptyDense, emptySparse, emptySorted
  }

}
