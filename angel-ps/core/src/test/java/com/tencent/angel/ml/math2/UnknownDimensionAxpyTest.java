package com.tencent.angel.ml.math2;

import com.tencent.angel.ml.math2.vector.IntDoubleVector;
import com.tencent.angel.ml.math2.vector.IntFloatVector;
import com.tencent.angel.ml.math2.utils.Constant;
import org.junit.Test;
import static org.junit.Assert.*;

public class UnknownDimensionAxpyTest {
  @Test public void doubleUnknownDimensionStaysSparseAndPreservesInputs() {
    boolean old = Constant.keepStorage;
    try {
      Constant.setKeepStorage(false);
      IntDoubleVector left = VFactory.sparseDoubleVector(-1, new int[]{1, 200}, new double[]{2, 3});
      IntDoubleVector right = VFactory.sparseDoubleVector(-1, new int[]{1, 500}, new double[]{4, 5});
      IntDoubleVector result = (IntDoubleVector) left.axpy(right, 2);
      assertEquals(-1, result.dim());
      assertFalse(result.isDense());
      assertEquals(10, result.get(1), 0);
      assertEquals(3, result.get(200), 0);
      assertEquals(10, result.get(500), 0);
      assertEquals(2, left.get(1), 0);
      assertEquals(4, right.get(1), 0);
    } finally { Constant.setKeepStorage(old); }
  }

  @Test public void floatInplaceUnknownDimensionDoesNotAllocateDense() {
    boolean old = Constant.keepStorage;
    try {
      Constant.setKeepStorage(false);
      IntFloatVector left = VFactory.sparseFloatVector(-1, new int[]{1, 200}, new float[]{2, 3});
      IntFloatVector right = VFactory.sparseFloatVector(-1, new int[]{1, 500}, new float[]{4, 5});
      left.iaxpy(right, 2);
      assertEquals(-1, left.dim());
      assertFalse(left.isDense());
      assertEquals(10, left.get(1), 0);
      assertEquals(10, left.get(500), 0);
      assertEquals(5, right.get(500), 0);
    } finally { Constant.setKeepStorage(old); }
  }
}
