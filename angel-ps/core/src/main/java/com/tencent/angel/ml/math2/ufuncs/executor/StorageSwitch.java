package com.tencent.angel.ml.math2.ufuncs.executor;

import com.tencent.angel.ml.math2.storage.*;
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.ufuncs.expression.OpType;
import com.tencent.angel.ml.math2.utils.Constant;
import com.tencent.angel.ml.math2.vector.*;

public class StorageSwitch {
  public static IntDoubleVectorStorage apply(IntDoubleVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sparseDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
			if (v1.size() + v2.getSize() < 1.5 * capacity) {
			  return v1.getStorage().copy();
			} else {
			  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
			}
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sortedDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
		  }
		}
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isDense()) {
	    if (v2.isDense()){
		  return v1.getStorage().copy();
		}else if (v2.isSparse()){
	      if (op.isKeepStorage() || v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()){
	        return v1.getStorage().emptyDense();
		  }else{
	        return v1.getStorage().emptySparse((int) v2.getSize());
		  }
		}else{//sorted
		  if (op.isKeepStorage() || (v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim() && v2.isSorted())){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySorted((int) v2.getSize());
		  }
		}
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
		    return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
		    return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
		    return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
		    return v1.getStorage().copy();
		  }else if (v2.isSorted()){
		    return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()){
		    return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
		    return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
		    return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else {
	    return v1.getStorage().emptyDense();
	  }
	}
  }

  public static IntFloatVectorStorage apply(IntFloatVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sparseDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
			if (v1.size() + v2.getSize() < 1.5 * capacity) {
			  return v1.getStorage().copy();
			} else {
			  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
			}
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sortedDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
		  }
		}
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isDense()) {
		if (v2.isDense()){
		  return v1.getStorage().copy();
		}else if (v2.isSparse()){
		  if (op.isKeepStorage() || v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }
		}else{//sorted
		  if (op.isKeepStorage() || (v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim() && v2.isSorted())){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySorted((int) v2.getSize());
		  }
		}
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else {
		return v1.getStorage().emptyDense();
	  }
	}
  }

  public static IntLongVectorStorage apply(IntLongVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sparseDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
			if (v1.size() + v2.getSize() < 1.5 * capacity) {
			  return v1.getStorage().copy();
			} else {
			  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
			}
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sortedDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
		  }
		}
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isDense()) {
		if (v2.isDense()){
		  return v1.getStorage().copy();
		}else if (v2.isSparse()){
		  if (op.isKeepStorage() || v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }
		}else{//sorted
		  if (op.isKeepStorage() || (v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim() && v2.isSorted())){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySorted((int) v2.getSize());
		  }
		}
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else {
		return v1.getStorage().emptyDense();
	  }
	}
  }

  public static IntIntVectorStorage apply(IntIntVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sparseDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
			if (v1.size() + v2.getSize() < 1.5 * capacity) {
			  return v1.getStorage().copy();
			} else {
			  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
			}
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  return v1.getStorage().emptyDense();
		} else {
		  if ((v1.size() + v2.getSize()) * Constant.intersectionCoeff
				  > Constant.sortedDenseStorageThreshold * v1.getDim()) {
			return v1.getStorage().emptyDense();
		  } else {
			return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
		  }
		}
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isDense()) {
		if (v2.isDense()){
		  return v1.getStorage().copy();
		}else if (v2.isSparse()){
		  if (op.isKeepStorage() || v2.getSize() > Constant.sparseDenseStorageThreshold * v2.dim()){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }
		}else{//sorted
		  if (op.isKeepStorage() || (v2.getSize() > Constant.sortedDenseStorageThreshold * v2.dim() && v2.isSorted())){
			return v1.getStorage().emptyDense();
		  }else{
			return v1.getStorage().emptySorted((int) v2.getSize());
		  }
		}
	  } else if (v1.isSparse()) {
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sparseDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isDense()) {
		  if (op.isKeepStorage() || v1.size() < Constant.sortedDenseStorageThreshold * v1.getDim()){
			return v1.getStorage().copy();
		  }else{
			return v1.getStorage().emptyDense();
		  }
		} else if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isDense()) {
		return v1.getStorage().copy();
	  } else {
		return v1.getStorage().emptyDense();
	  }
	}
  }

  public static LongDoubleVectorStorage apply(LongDoubleVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isSparse()) {
		int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
		if (v1.size() + v2.getSize() < 1.5 * capacity) {
		  return v1.getStorage().copy();
		} else {
		  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
		}
	  } else {//sorted
		return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isSparse()) {
		if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isSorted() && v2.isSorted()) {
		return v1.getStorage().emptySorted((int) (v1.getDim()));
	  } else {
		return v1.getStorage().emptySparse((int) (v1.getDim()));
	  }
	}
  }

  public static LongFloatVectorStorage apply(LongFloatVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isSparse()) {
		int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
		if (v1.size() + v2.getSize() < 1.5 * capacity) {
		  return v1.getStorage().copy();
		} else {
		  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
		}
	  } else {//sorted
		return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isSparse()) {
		if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isSorted() && v2.isSorted()) {
		return v1.getStorage().emptySorted((int) (v1.getDim()));
	  } else {
		return v1.getStorage().emptySparse((int) (v1.getDim()));
	  }
	}
  }

  public static LongLongVectorStorage apply(LongLongVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isSparse()) {
		int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
		if (v1.size() + v2.getSize() < 1.5 * capacity) {
		  return v1.getStorage().copy();
		} else {
		  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
		}
	  } else {//sorted
		return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isSparse()) {
		if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isSorted() && v2.isSorted()) {
		return v1.getStorage().emptySorted((int) (v1.getDim()));
	  } else {
		return v1.getStorage().emptySparse((int) (v1.getDim()));
	  }
	}
  }

  public static LongIntVectorStorage apply(LongIntVector v1, Vector v2, Binary op) {
	if (op.getOpType() == OpType.UNION) {
	  if (v1.isSparse()) {
		int capacity = 1 << (32 - Integer.numberOfLeadingZeros((int) (v1.size() / 0.75)));
		if (v1.size() + v2.getSize() < 1.5 * capacity) {
		  return v1.getStorage().copy();
		} else {
		  return v1.getStorage().emptySparse((int) (v1.size() + v2.getSize()));
		}
	  } else {//sorted
		return v1.getStorage().emptySorted((int) (v1.size() + v2.getSize()));
	  }
	} else if (op.getOpType() == OpType.INTERSECTION){
	  if (v1.isSparse()) {
		if (v2.isSparse()){
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySparse((int) v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}else{
		  if ((op.isKeepStorage() || v1.size() < v2.getSize() * Constant.sparseSortedThreshold) && v2.isSorted()){
			return v1.getStorage().copy();
		  }else if ((v1.size() > v2.getSize() && !op.isKeepStorage()) && !v2.isSorted()){
			return v1.getStorage().copy();
		  }else if (v2.isSorted()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  } else {//sorted
		if (v2.isSparse()){
		  if (op.isKeepStorage() || v1.size() * Constant.sparseSortedThreshold < v2.getSize()) {
			return v1.getStorage().copy();
		  } else {
			return v1.getStorage().emptySparse((int) (v2.getSize()));
		  }
		}else{//v2 is sorted
		  if (v1.size() > v2.getSize()){
			return v1.getStorage().emptySorted((int)v2.getSize());
		  }else{
			return v1.getStorage().copy();
		  }
		}
	  }
	}else{//OpType.ALL
	  if (v1.isSorted() && v2.isSorted()) {
		return v1.getStorage().emptySorted((int) (v1.getDim()));
	  } else {
		return v1.getStorage().emptySparse((int) (v1.getDim()));
	  }
	}
  }

  public static IntDoubleVector[] apply(CompIntDoubleVector v1, Vector v2, Binary op){
	IntDoubleVector[] parts = v1.getPartitions();
	IntDoubleVector[] resParts = new IntDoubleVector[parts.length];
	int k = 0;
    if (op.getOpType() == OpType.UNION){
      if (v2.isDense()){
		for (IntDoubleVector part :parts) {
		  IntDoubleVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntDoubleVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntDoubleSparseVectorStorage sto =
					new IntDoubleSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
      if (v2.isDense()){
		resParts = v1.copy().getPartitions();
	  }else {
        if (v1.size() > v2.getSize() || (!v2.isSparse() && ! v2.isSorted())){
		  for (IntDoubleVector part : parts) {
			resParts[k] =
					new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							part.getStorage().emptySparse());
			k++;
		  }
		}else {
		  resParts = v1.copy().getPartitions();
		}
	  }
	}else{//OpType.ALL
      if (v2.isDense()){
		for (IntDoubleVector part : parts) {
		  IntDoubleVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntDoubleVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntDoubleSparseVectorStorage sto =
					new IntDoubleSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}
    return resParts;
  }

  public static IntFloatVector[] apply(CompIntFloatVector v1, Vector v2, Binary op){
	IntFloatVector[] parts = v1.getPartitions();
	IntFloatVector[] resParts = new IntFloatVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  if (v2.isDense()){
		for (IntFloatVector part :parts) {
		  IntFloatVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntFloatVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntFloatSparseVectorStorage sto =
					new IntFloatSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v2.isDense()){
		resParts = v1.copy().getPartitions();
	  }else {
		if (v1.size() > v2.getSize()){
		  for (IntFloatVector part : parts) {
			resParts[k] =
					new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							part.getStorage().emptySparse());
			k++;
		  }
		}else {
		  resParts = v1.copy().getPartitions();
		}
	  }
	}else{//OpType.ALL
	  if (v2.isDense()){
		for (IntFloatVector part : parts) {
		  IntFloatVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntFloatVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntFloatSparseVectorStorage sto =
					new IntFloatSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}
	return resParts;
  }

  public static IntLongVector[] apply(CompIntLongVector v1, Vector v2, Binary op){
	IntLongVector[] parts = v1.getPartitions();
	IntLongVector[] resParts = new IntLongVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  if (v2.isDense()){
		for (IntLongVector part :parts) {
		  IntLongVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntLongVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntLongSparseVectorStorage sto =
					new IntLongSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v2.isDense()){
		resParts = v1.copy().getPartitions();
	  }else {
		if (v1.size() > v2.getSize()){
		  for (IntLongVector part : parts) {
			resParts[k] =
					new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							part.getStorage().emptySparse());
			k++;
		  }
		}else {
		  resParts = v1.copy().getPartitions();
		}
	  }
	}else{//OpType.ALL
	  if (v2.isDense()){
		for (IntLongVector part : parts) {
		  IntLongVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntLongVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntLongSparseVectorStorage sto =
					new IntLongSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}
	return resParts;
  }

  public static IntIntVector[] apply(CompIntIntVector v1, Vector v2, Binary op){
	IntIntVector[] parts = v1.getPartitions();
	IntIntVector[] resParts = new IntIntVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  if (v2.isDense()){
		for (IntIntVector part :parts) {
		  IntIntVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntIntVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntIntSparseVectorStorage sto =
					new IntIntSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v2.isDense()){
		resParts = v1.copy().getPartitions();
	  }else {
		if (v1.size() > v2.getSize()){
		  for (IntIntVector part : parts) {
			resParts[k] =
					new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							part.getStorage().emptySparse());
			k++;
		  }
		}else {
		  resParts = v1.copy().getPartitions();
		}
	  }
	}else{//OpType.ALL
	  if (v2.isDense()){
		for (IntIntVector part : parts) {
		  IntIntVectorStorage newStorage = part.getStorage().emptyDense();
		  resParts[k] =
				  new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  newStorage);
		  k++;
		}
	  }else {
		for (IntIntVector part : parts) {
		  if (part.isDense() || part.isSparse()) {
			resParts[k] = part.copy();
		  } else { // sorted
			IntIntSparseVectorStorage sto =
					new IntIntSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
							part.getStorage().getValues());
			resParts[k] =
					new IntIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
							sto);
		  }
		  k++;
		}
	  }
	}
	return resParts;
  }

  public static LongDoubleVector[] apply(CompLongDoubleVector v1, Vector v2, Binary op){
	LongDoubleVector[] parts = v1.getPartitions();
	LongDoubleVector[] resParts = new LongDoubleVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  for (LongDoubleVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongDoubleSparseVectorStorage sto =
				  new LongDoubleSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v1.size() > v2.getSize()){
		for (LongDoubleVector part : parts) {
		  resParts[k] =
				  new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  part.getStorage().emptySparse());
		  k++;
		}
	  }else {
		resParts = v1.copy().getPartitions();
	  }
	}else{//OpType.ALL
	  for (LongDoubleVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongDoubleSparseVectorStorage sto =
				  new LongDoubleSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongDoubleVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}
	return resParts;
  }

  public static LongFloatVector[] apply(CompLongFloatVector v1, Vector v2, Binary op){
	LongFloatVector[] parts = v1.getPartitions();
	LongFloatVector[] resParts = new LongFloatVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  for (LongFloatVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongFloatSparseVectorStorage sto =
				  new LongFloatSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v1.size() > v2.getSize()){
		for (LongFloatVector part : parts) {
		  resParts[k] =
				  new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  part.getStorage().emptySparse());
		  k++;
		}
	  }else {
		resParts = v1.copy().getPartitions();
	  }
	}else{//OpType.ALL
	  for (LongFloatVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongFloatSparseVectorStorage sto =
				  new LongFloatSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongFloatVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}
	return resParts;
  }

  public static LongLongVector[] apply(CompLongLongVector v1, Vector v2, Binary op){
	LongLongVector[] parts = v1.getPartitions();
	LongLongVector[] resParts = new LongLongVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  for (LongLongVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongLongSparseVectorStorage sto =
				  new LongLongSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v1.size() > v2.getSize()){
		for (LongLongVector part : parts) {
		  resParts[k] =
				  new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  part.getStorage().emptySparse());
		  k++;
		}
	  }else {
		resParts = v1.copy().getPartitions();
	  }
	}else{//OpType.ALL
	  for (LongLongVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongLongSparseVectorStorage sto =
				  new LongLongSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongLongVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}
	return resParts;
  }

  public static LongIntVector[] apply(CompLongIntVector v1, Vector v2, Binary op){
	LongIntVector[] parts = v1.getPartitions();
	LongIntVector[] resParts = new LongIntVector[parts.length];
	int k = 0;
	if (op.getOpType() == OpType.UNION){
	  for (LongIntVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongIntSparseVectorStorage sto =
				  new LongIntSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}else if(op.getOpType() == OpType.INTERSECTION){
	  if (v1.size() > v2.getSize()){
		for (LongIntVector part : parts) {
		  resParts[k] =
				  new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  part.getStorage().emptySparse());
		  k++;
		}
	  }else {
		resParts = v1.copy().getPartitions();
	  }
	}else{//OpType.ALL
	  for (LongIntVector part : parts) {
		if (part.isDense() || part.isSparse()) {
		  resParts[k] = part.copy();
		} else { // sorted
		  LongIntSparseVectorStorage sto =
				  new LongIntSparseVectorStorage(part.getDim(), part.getStorage().getIndices(),
						  part.getStorage().getValues());
		  resParts[k] =
				  new LongIntVector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(),
						  sto);
		}
		k++;
	  }
	}
	return resParts;
  }
}
