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


package com.tencent.angel.ml.math2.ufuncs.executor2.matrix;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;


public class DotMatrixExecutor {
  private volatile static DotExecutor executor;
  private static String MatrixVsDummyVector = "{\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "&dtype1Low[] resArr;\n" +
		  "if (trans) {\n" +
		  "	Assert.check(m == v.getDim());\n" +
		  "	resArr = new &dtype1Low[n];\n" +
		  "} else {\n" +
		  "	Assert.check(n == v.getDim());\n" +
		  "	resArr = new &dtype1Low[m];\n" +
		  "}\n" +
		  "int r = mat.getNumRows(), c = mat.getNumCols();\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "if (trans) {\n" +
		  "	for (int j = 0; j < c; j++) {\n" +
		  "		int[] idxs = v.getIndices();\n" +
		  "		for (int i = 0; i < idxs.length; i++) {\n" +
		  "			resArr[j] += data[idxs[i] * c + j];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else {\n" +
		  "	for (int i = 0; i < r; i++) {\n" +
		  "		int[] idxs = v.getIndices();\n" +
		  "		for (int j = 0; j < idxs.length; j++) {\n" +
		  "			resArr[i] += data[i * c + idxs[j]];\n" +
		  "		}\n" +
		  "	}\n" +
		  "}\n" +
		  "Int&dtype1DenseVectorStorage storage = new Int&dtype1DenseVectorStorage(resArr);\n" +
		  "return new Int&dtype1Vector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);\n" +
		  "}\n\n";
  private static String MatrixVsVector = "{\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "&dtype1Low[] resArr;\n" +
		  "if (trans) {\n" +
		  "	Assert.check(m == v.getDim());\n" +
		  "	resArr = new &dtype1Low[n];\n" +
		  "} else {\n" +
		  "	Assert.check(n == v.getDim());\n" +
		  "	resArr = new &dtype1Low[m];\n" +
		  "}\n" +
		  "int r = mat.getNumRows(), c = mat.getNumCols();\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "if (v.isDense()) {\n" +
		  "	&dtype1Low[] tempArray = ArrayCopy.copy(v.getStorage().getValues(), new &dtype1Low[v.getDim()]);\n" +
		  "	if (trans) {\n" +
		  "		blas.&replace(\"N\", c, r, (&dtype1Low)1, data, c, tempArray, 1, (&dtype1Low)0, resArr, 1);\n" +
		  "	} else {\n" +
		  "		blas.&replace(\"T\", c, r, (&dtype1Low)1, data, c, tempArray, 1, (&dtype1Low)0, resArr, 1);\n" +
		  "	}\n" +
		  "} else if (v.isSparse()) {\n" +
		  "	if (trans) {\n" +
		  "		MatrixCommonFuncs.getInstance().matrixTransSparse(mat, v, resArr);\n" +
		  "	} else {\n" +
		  "		MatrixCommonFuncs.getInstance().matrixNotTransSparse(mat, v, resArr);\n" +
		  "	}\n" +
		  "} else { // sorted\n" +
		  "	if (trans) {\n" +
		  "		MatrixCommonFuncs.getInstance().matrixTransSorted(mat, v, resArr);\n" +
		  "	} else {\n" +
		  "		MatrixCommonFuncs.getInstance().matrixNotTransSorted(mat, v, resArr);\n" +
		  "	}\n" +
		  "}\n" +
		  "Int&dtype1DenseVectorStorage storage = new Int&dtype1DenseVectorStorage(resArr);\n" +
		  "return new Int&dtype1Vector(v.getMatrixId(), v.getClock(), 0, resArr.length, storage);\n" +
		  "}\n\n";
  private static String BlasMatrixVsBlasMatrix = "{\n" +
		  "return MatrixCommonFuncs.getInstance().blasVsBlas(mat1, trans1, mat2, trans2, false);\n" +
		  "}\n\n";
  private static String BlasMatrixVsRBMatrix = "{\n" +
		  "int dim = (int) mat2.getDim();\n" +
		  "int subDim = mat2.getSubDim();\n" +
		  "int resNumRows, resDim;\n" +
		  "CompInt&dtype1Vector[] resRows;\n" +
		  "Blas&dtype1Matrix newMatrix = (Blas&dtype1Matrix)MatrixCommonFuncs.getInstance().rbMatrix2BlasMatrix(mat2);\n" +
		  "newMatrix = (Blas&dtype1Matrix)MatrixCommonFuncs.getInstance().blasVsBlas(mat1, trans1, newMatrix, trans2, true);\n" +
		  "&dtype1Low[] resBlas = newMatrix.getData();\n" +
		  "resNumRows = newMatrix.getNumRows();\n" +
		  "resDim = newMatrix.getNumCols();\n" +
		  "int numComp = (resDim + subDim - 1) / subDim;\n" +
		  "resRows = new CompInt&dtype1Vector[resNumRows];\n" +
		  "for (int row = 0; row < resNumRows; row++) {\n" +
		  "	Int&dtype1Vector[] parts = new Int&dtype1Vector[numComp];\n" +
		  "	for (int i = 0; i < numComp; i++) {\n" +
		  "		int thisSubDim;\n" +
		  "		if ((i + 1) * subDim > resDim) {\n" +
		  "			thisSubDim = dim - i * subDim;\n" +
		  "		} else {\n" +
		  "			thisSubDim = subDim;\n" +
		  "		}\n" +
		  "		&dtype1Low[] part = new &dtype1Low[thisSubDim];\n" +
		  "		System.arraycopy(resBlas, row * dim + i * subDim, part, 0, thisSubDim);\n" +
		  "		parts[i] = VFactory.dense&dtype1Vector(part);\n" +
		  "	}\n" +
		  "resRows[row] = VFactory.compInt&dtype1Vector(resDim, parts);\n" +
		  "}\n" +
		  "return MFactory.rbCompInt&dtype1Matrix(resRows);\n" +
		  "}\n\n";
  private static String RBMatrixVsBlasMatrix = "{\n" +
		  "Blas&dtype1Matrix newMatrix = (Blas&dtype1Matrix)MatrixCommonFuncs.getInstance().rbMatrix2BlasMatrix(mat1);\n" +
		  "return MatrixCommonFuncs.getInstance().blasVsBlas(newMatrix, trans1, mat2, trans2, false);\n" +
		  "}\n";
  private static String MatrixVectorDummy = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "Assert.check(m == v1.getDim() && n == v2.getDim());\n" +
		  "if (v1.isDense()) {\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	int size = v2.size();\n" +
		  "	for (int i = 0; i < m; i++) { // row\n" +
		  "		int[] v2Idxs = v2.getIndices();\n" +
		  "		for (int k = 0; k < size; k++) {\n" +
		  "			int j = v2Idxs[k]; // col\n" +
		  "			data[i * n + j] += alpha * v1Values[i];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSparse()) {\n" +
		  "	int[] v2Idxs = v2.getIndices();\n" +
		  "	ObjectIterator iter = v1.getStorage().entryIterator();\n" +
		  "	int size = v2.size();\n" +
		  "	while (iter.hasNext()) {\n" +
		  "		Int2&dtype1Map.Entry entry = (Int2&dtype1Map.Entry)iter.next();\n" +
		  "		int i = entry.getIntKey(); // row\n" +
		  "		for (int k = 0; k < size; k++) {\n" +
		  "			int j = v2Idxs[k]; // col\n" +
		  "			data[i * n + j] += alpha * entry.get&dtype1Value();\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else {\n" +
		  "	int[] v1Idxs = v1.getStorage().getIndices();\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	int[] v2Idxs = v2.getIndices();\n" +
		  "	int size1 = v1.size();\n" +
		  "	int size2 = v2.size();\n" +
		  "	for (int k = 0; k < size1; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		for (int t = 0; t < size2; t++) {\n" +
		  "			int j = v2Idxs[t];\n" +
		  "			data[i * n + j] += alpha * v1Values[k];\n" +
		  "		}\n" +
		  "	}\n" +
		  "}\n" +
		  "return mat;\n" +
		  "}\n\n";
  private static String MatrixDummyVector = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "Assert.check(m == v1.getDim() && n == v2.getDim());\n" +
		  "if (v2.isDense()) {\n" +
		  "	int[] v1Idxs = v1.getIndices();\n" +
		  "	&dtype1Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	int size = v1.size();\n" +
		  "	for (int k = 0; k < size; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		for (int j = 0; j < n; j++) {\n" +
		  "			data[i * n + j] += alpha * v2Values[j];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v2.isSparse()) {\n" +
		  "	int[] v1Idxs = v1.getIndices();\n" +
		  "	int size = v1.size();\n" +
		  "	for (int k = 0; k < size; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		ObjectIterator iter = v2.getStorage().entryIterator();\n" +
		  "		while (iter.hasNext()) {\n" +
		  "			Int2&dtype1Map.Entry entry = (Int2&dtype1Map.Entry)iter.next();\n" +
		  "			int j = entry.getIntKey();\n" +
		  "			data[i * n + j] += alpha * entry.get&dtype1Value();\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v2.isSorted()) {\n" +
		  "	int[] v1Idxs = v1.getIndices();\n" +
		  "	int[] v2Idxs = v2.getStorage().getIndices();\n" +
		  "	&dtype1Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	int size1 = v1.size();\n" +
		  "	int size2 = v2.size();\n" +
		  "	for (int k = 0; k < size1; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		for (int t = 0; t < size2; t++) {\n" +
		  "			int j = v2Idxs[t];\n" +
		  "			data[i * n + j] += alpha * v2Values[t];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else {\n" +
		  "	throw new AngelException(\"The operation is not supported!\");\n" +
		  "}\n" +
		  "return mat;\n" +
		  "}\n\n";
  private static String MatrixVectorVector = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "Assert.check(m == v1.getDim() && n == v2.getDim());\n" +
		  "if (v1.isDense() && v2.isDense()) {\n" +
		  "	&dtype1Low[] v1Value = v1.getStorage().getValues();\n" +
		  "	&dtype2Low[] v2Value = v2.getStorage().getValues();\n" +
		  "	blas.&replace(n, m, alpha, v2Value, 1, v1Value, 1, data, n);\n" +
		  "	mat.setData(data);\n" +
		  "} else if (v1.isDense() && v2.isSparse()) {\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	for (int i = 0; i < m; i++) { // row\n" +
		  "		ObjectIterator iter = v2.getStorage().entryIterator();\n" +
		  "     while (iter.hasNext()) {\n" +
		  "     	Int2&dtype2Map.Entry entry = (Int2&dtype2Map.Entry)iter.next();\n" +
		  "			int j = entry.getIntKey(); // col\n" +
		  "			data[i * n + j] += alpha * v1Values[i] * entry.get&dtype2Value();\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isDense() && v2.isSorted()) {\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	int size = v2.size();\n" +
		  "	for (int i = 0; i < m; i++) { // row\n" +
		  "		int[] v2Idxs = v2.getStorage().getIndices();\n" +
		  "		&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "		for (int k = 0; k < size; k++) {\n" +
		  "			int j = v2Idxs[k]; // col\n" +
		  "			data[i * n + j] += alpha * v1Values[i] * v2Values[k];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSparse() && v2.isDense()) {\n" +
		  "	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	ObjectIterator iter = v1.getStorage().entryIterator();\n" +
		  "	while (iter.hasNext()) {\n" +
		  "		Int2&dtype1Map.Entry entry = (Int2&dtype1Map.Entry)iter.next();\n" +
		  "		int i = entry.getIntKey(); // row\n" +
		  "		for (int j = 0; j < n; j++) {\n" +
		  "			data[i * n + j] += alpha * entry.get&dtype1Value() * v2Values[j];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSparse() && v2.isSparse()) {\n" +
		  "	ObjectIterator outer = v1.getStorage().entryIterator();\n" +
		  "	while (outer.hasNext()) {\n" +
		  "		Int2&dtype1Map.Entry entry1 = (Int2&dtype1Map.Entry)outer.next();\n" +
		  "		int i = entry1.getIntKey(); // row\n" +
		  "		ObjectIterator inner = v2.getStorage().entryIterator();\n" +
		  "		while (inner.hasNext()) {\n" +
		  "			Int2&dtype2Map.Entry entry2 = (Int2&dtype2Map.Entry)inner.next();\n" +
		  "			int j = entry2.getIntKey(); // col\n" +
		  "			data[i * n + j] += alpha * entry1.get&dtype1Value() * entry2.get&dtype2Value();\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSparse() && v2.isSorted()) {\n" +
		  "	int[] v2Idxs = v2.getStorage().getIndices();\n" +
		  "	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	ObjectIterator iter = v1.getStorage().entryIterator();\n" +
		  "	int size = v2.size();\n" +
		  "	while (iter.hasNext()) {\n" +
		  "		Int2&dtype1Map.Entry entry = (Int2&dtype1Map.Entry)iter.next();\n" +
		  "		int i = entry.getIntKey(); // row\n" +
		  "		for (int k = 0; k < size; k++) {\n" +
		  "			int j = v2Idxs[k]; // col\n" +
		  "			data[i * n + j] += alpha * entry.get&dtype1Value() * v2Values[k];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSorted() && v2.isDense()) {\n" +
		  "	int[] v1Idxs = v1.getStorage().getIndices();\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	int size = v1.size();\n" +
		  "	for (int k = 0; k < size; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		for (int j = 0; j < n; j++) {\n" +
		  "			data[i * n + j] += alpha * v1Values[k] * v2Values[j];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSorted() && v2.isSparse()) {\n" +
		  "	int[] v1Idxs = v1.getStorage().getIndices();\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	int size = v1.size();\n" +
		  "	for (int k = 0; k < size; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		ObjectIterator iter = v2.getStorage().entryIterator();\n" +
		  "		while (iter.hasNext()) {\n" +
		  "			Int2&dtype2Map.Entry entry = (Int2&dtype2Map.Entry)iter.next();\n" +
		  "			int j = entry.getIntKey();\n" +
		  "			data[i * n + j] += alpha * v1Values[k] * entry.get&dtype2Value();\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else if (v1.isSorted() && v2.isSorted()) {\n" +
		  "	int[] v1Idxs = v1.getStorage().getIndices();\n" +
		  "	&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "	int[] v2Idxs = v2.getStorage().getIndices();\n" +
		  "	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	int size1 = v1.size();\n" +
		  "	int size2 = v2.size();\n" +
		  "	for (int k = 0; k < size1; k++) {\n" +
		  "		int i = v1Idxs[k];\n" +
		  "		for (int t = 0; t < size2; t++) {\n" +
		  "			int j = v2Idxs[t];\n" +
		  "			data[i * n + j] += alpha * v1Values[k] * v2Values[t];\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else {\n" +
		  "	throw new AngelException(\"The operation is not supported!\");\n" +
		  "}\n" +
		  "return mat;\n" +
		  "}\n\n";

  public static Vector apply(Matrix mat, boolean trans, Vector v) {
	return getInstance().apply(mat, trans, v);
  }

  public static Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2) {
	return getInstance().apply(mat1, trans1, mat2, trans2);
  }

  public static double apply(Matrix mat, Vector v1) {
	return getInstance().apply(mat, v1);
  }

  public static double apply(Matrix mat, Vector v1, Vector v2) {
	return getInstance().apply(mat, v1, v2);
  }

  public static Matrix apply(Matrix mat, double alpha, Vector v1, Vector v2) {
	return getInstance().apply(mat, alpha, v1, v2);
  }

  private static DotExecutor getInstance() {
	if (executor == null) {
	  synchronized (DotExecutor.class) {
		if (executor == null) {
		  try {
			String[] dtypes1 = new String[]{"Double", "Float"};
			String[] dtypes2 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String[] names = new String[]{"MatrixVsDummyVector", "MatrixVsVector", "BlasMatrixVsBlasMatrix",
					"MatrixVectorDummy", "MatrixDummyVector", "MatrixVectorVector", "BlasMatrixVsRBMatrix", "" +
					"RBMatrixVsBlasMatrix"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.matrix.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.tencent.angel.ml.math2.MFactory;\n" +
					"import com.tencent.angel.ml.math2.VFactory;\n" +
					"import com.tencent.angel.ml.math2.matrix.*;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import com.github.fommil.netlib.BLAS;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n" +
					"import com.sun.tools.javac.util.Assert;\n" +
					"import com.tencent.angel.ml.math2.utils.ArrayCopy;\n\n" +
					"private static BLAS blas = BLAS.getInstance();\n" +
					"public Vector apply(Matrix mat, boolean trans, Vector v) {\n" +
					"    if (mat instanceof BlasDoubleMatrix && v instanceof IntDoubleVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, trans, (IntDoubleVector) v);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntFloatVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, trans, (IntFloatVector) v);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntLongVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, trans, (IntLongVector) v);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntIntVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, trans, (IntIntVector) v);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntDummyVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, trans, (IntDummyVector) v);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntFloatVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, trans, (IntFloatVector) v);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntLongVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, trans, (IntLongVector) v);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntIntVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, trans, (IntIntVector) v);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntDummyVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, trans, (IntDummyVector) v);\n" +
					"    } else if (mat instanceof RowBasedMatrix) {\n" +
					"      if (trans) {\n" +
					"        return ((RowBasedMatrix) mat).transDot(v);\n" +
					"      } else {\n" +
					"        return mat.dot(v);\n" +
					"      }\n" +
					"    } else {\n" +
					"      throw new AngelException(\"the operation is not supported!\");\n" +
					"    }\n" +
					"  }\n\n" +
					"public Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2) {\n" +
					"    if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof BlasDoubleMatrix) {\n" +
					"      return apply((BlasDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2);\n" +
					"    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof BlasFloatMatrix) {\n" +
					"      return apply((BlasFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2);\n" +
					"    } else if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof RBCompIntDoubleMatrix) {\n" +
					"      return apply((BlasDoubleMatrix) mat1, trans1, (RBCompIntDoubleMatrix) mat2, trans2);\n" +
					"    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof RBCompIntFloatMatrix) {\n" +
					"      return apply((BlasFloatMatrix) mat1, trans1, (RBCompIntFloatMatrix) mat2, trans2);\n" +
					"    } else if (mat1 instanceof RBCompIntDoubleMatrix && mat2 instanceof BlasDoubleMatrix) {\n" +
					"      return apply((RBCompIntDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2);\n" +
					"    } else if (mat1 instanceof RBCompIntFloatMatrix && mat2 instanceof BlasFloatMatrix) {\n" +
					"      return apply((RBCompIntFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2);\n" +
					"    } else if (mat1 instanceof RowBasedMatrix && mat2 instanceof RowBasedMatrix) {\n" +
					"      Assert.check(trans1 && !trans2);\n" +
					"      return mat1.dot(mat1);\n" +
					"    } else {\n" +
					"      throw new AngelException(\"the operation is not supported!\");\n" +
					"    }\n" +
					"  }\n\n" +
					"public double apply(Matrix mat, Vector v1) {\n" +
					"    if (mat instanceof BlasMatrix) {\n" +
					"      Assert.check(((BlasMatrix) mat).getNumRows() == ((BlasMatrix) mat).getNumCols());\n" +
					"      return apply(mat, true, v1).dot(v1);\n" +
					"    } else {\n" +
					"      return mat.dot(v1).dot(v1);\n" +
					"    }\n" +
					"  }\n\n" +
					"public double apply(Matrix mat, Vector v1, Vector v2) {\n" +
					"    if (mat instanceof BlasMatrix) {\n" +
					"      if (!v2.isDense()) {\n" +
					"        return apply(mat, false, v2).dot(v1);\n" +
					"      } else {\n" +
					"        return apply(mat, true, v1).dot(v2);\n" +
					"      }\n" +
					"    } else {\n" +
					"      return mat.dot(v2).dot(v1);\n" +
					"    }\n" +
					"  }\n\n" +
					"public Matrix apply(Matrix mat, double alpha, Vector v1, Vector v2) {\n" +
					"    Assert.check(mat instanceof BlasMatrix);\n" +
					"    if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDoubleVector\n" +
					"      && v2 instanceof IntDoubleVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, alpha, (IntDoubleVector) v1, (IntDoubleVector) v2);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDummyVector\n" +
					"      && v2 instanceof IntDoubleVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, alpha, (IntDummyVector) v1, (IntDoubleVector) v2);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDoubleVector\n" +
					"      && v2 instanceof IntDummyVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, alpha, (IntDoubleVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v1 instanceof IntDummyVector\n" +
					"      && v2 instanceof IntDummyVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, alpha, (IntDummyVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntFloatVector\n" +
					"      && v2 instanceof IntFloatVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (float) alpha, (IntFloatVector) v1, (IntFloatVector) v2);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntDummyVector\n" +
					"      && v2 instanceof IntFloatVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (float) alpha, (IntDummyVector) v1, (IntFloatVector) v2);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntFloatVector\n" +
					"      && v2 instanceof IntDummyVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (float) alpha, (IntFloatVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v1 instanceof IntDummyVector\n" +
					"      && v2 instanceof IntDummyVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (float) alpha, (IntDummyVector) v1, (IntDummyVector) v2);\n" +
					"    } else {\n" +
					"      throw new AngelException(\"The operation is not supported!\");\n" +
					"    }\n" +
					"  }\n\n";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare = "", body = "", method;
			for (int k = 0; k < names.length; k++) {
			  for (int i = 0; i < dtypes1.length; i++) {
				if (names[k] == "MatrixVectorVector") {
				  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
						  .replaceAll("&Parameters", "Blas&dtype1Matrix mat, &dtype1Low alpha, " +
								  "Int&dtype1Vector v1, Int&dtype1Vector v2");
				  if (dtypes1[i] == "Double") {
					body = MatrixVectorVector.replaceAll("&replace", "dger");
				  } else {
					body = MatrixVectorVector.replaceAll("&replace", "sger");
				  }
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[i])
						  .replaceAll("&dtype2", dtypes1[i]);
				  template += method;
				} else if (names[k] == "BlasMatrixVsBlasMatrix") {
				  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
						  .replaceAll("&Parameters", "Blas&dtype1Matrix mat1, boolean trans1, " +
								  "Blas&dtype2Matrix mat2, boolean trans2");
				  if (dtypes1[i] == "Double") {
					body = BlasMatrixVsBlasMatrix.replaceAll("&replace", "dgemm");
				  } else {
					body = BlasMatrixVsBlasMatrix.replaceAll("&replace", "sgemm");
				  }
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[i])
						  .replaceAll("&dtype2", dtypes1[i]);
				  template += method;
				} else if (names[k] == "RBMatrixVsBlasMatrix") {
				  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
						  .replaceAll("&Parameters", "RBCompInt&dtype1Matrix mat1, boolean trans1, " +
								  "Blas&dtype2Matrix mat2, boolean trans2");
				  body = RBMatrixVsBlasMatrix;

				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[i])
						  .replaceAll("&dtype2", dtypes1[i]);
				  template += method;
				} else if (names[k] == "BlasMatrixVsRBMatrix") {
				  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
						  .replaceAll("&Parameters", "Blas&dtype1Matrix mat1, boolean trans1, " +
								  "RBCompInt&dtype2Matrix mat2, boolean trans2");
				  body = BlasMatrixVsRBMatrix;

				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[i])
						  .replaceAll("&dtype2", dtypes1[i]);
				  template += method;
				}

				for (int j = i; j < dtypes2.length; j++) {
				  if (names[k] == "MatrixVsDummyVector" && dtypes2[j] == "Dummy") {
					declare = declTemplate.replaceAll("&ReturnType", "Vector")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, boolean trans, Int&dtype2Vector v");
					body = MatrixVsDummyVector;
				  } else if (names[k] == "MatrixVsVector" && dtypes2[j] != "Dummy") {
					declare = declTemplate.replaceAll("&ReturnType", "Vector")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, boolean trans, Int&dtype2Vector v");
					if (dtypes1[i] == "Double") {
					  body = MatrixVsVector.replaceAll("&replace", "dgemv");
					} else {
					  body = MatrixVsVector.replaceAll("&replace", "sgemv");
					}
				  } else if ((dtypes1[i] == "Double" || dtypes1[i] == "Float") && dtypes2[j] == "Dummy") {
					if (names[k] == "MatrixVectorDummy") {
					  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							  .replaceAll("&Parameters", "Blas&dtype1Matrix mat, &dtype1Low alpha, " +
									  "Int&dtype1Vector v1, Int&dtype2Vector v2");
					  body = MatrixVectorDummy;
					} else if (names[k] == "MatrixDummyVector") {
					  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							  .replaceAll("&Parameters", "Blas&dtype1Matrix mat, &dtype1Low alpha, " +
									  "Int&dtype2Vector v1, Int&dtype1Vector v2");
					  body = MatrixDummyVector;
					}
				  } else {
					declare = "";
					body = "";
				  }

				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes2[j].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[i])
						  .replaceAll("&dtype2", dtypes2[j]);
				  template += method;
				  declare = "";
				  body = "";
				}
			  }
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (DotExecutor) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					DotExecutor.class,
					null
			);
		  } catch (Exception e) {
			e.printStackTrace();
		  }
		}
	  }
	}
	return executor;
  }
}
