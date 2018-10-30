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
import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;


public class BinaryMatrixExecutor {
  private volatile static MatrixExecutor executor;
  private static String matrixVsVectorByIndex = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "if (onCol && op.isInplace()) {\n" +
		  "  if (v.isDense()) {\n" +
		  "MatrixCommonFuncs.getInstance().onColDense(mat, v, mat, idx, op);\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "MatrixCommonFuncs.getInstance().onColSparse(mat, v, mat, idx, op);\n" +
		  "  } else { // sorted\n" +
		  "MatrixCommonFuncs.getInstance().onColSorted(mat, v, mat, idx, op);\n" +
		  "  }\n" +
		  "  return mat;\n" +
		  "} else if (onCol && !op.isInplace()) {\n" +
		  "  &dtype1Low[] newData;\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION) {\n" +
		  "newData = new &dtype1Low[m * n];\n" +
		  "  } else {\n" +
		  "newData = ArrayCopy.copy(data);\n" +
		  "  }\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  if (v.isDense()) {\n" +
		  "MatrixCommonFuncs.getInstance().onColDense(mat, v, newMat, idx, op);\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "MatrixCommonFuncs.getInstance().onColSparse(mat, v, newMat, idx, op);\n" +
		  "  } else { // sorted\n" +
		  "MatrixCommonFuncs.getInstance().onColSorted(mat, v, newMat, idx, op);\n" +
		  "  }\n" +
		  "  return newMat;\n" +
		  "} else if (!onCol && op.isInplace()) {\n" +
		  "  if (v.isDense()) {\n" +
		  "MatrixCommonFuncs.getInstance().onRowDense(mat, v, mat, idx, op);\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "MatrixCommonFuncs.getInstance().onRowSparse(mat, v, mat, idx, op);\n" +
		  "  } else { // sorted\n" +
		  "MatrixCommonFuncs.getInstance().onRowSorted(mat, v, mat, idx, op);\n" +
		  "  }\n" +
		  "  return mat;\n" +
		  "} else {\n" +
		  "  &dtype1Low[] newData;\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION) {\n" +
		  "newData = new &dtype1Low[m * n];\n" +
		  "  } else {\n" +
		  "newData = ArrayCopy.copy(data);\n" +
		  "  }\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  if (v.isDense()) {\n" +
		  "MatrixCommonFuncs.getInstance().onRowDense(mat, v, newMat, idx, op);\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "MatrixCommonFuncs.getInstance().onRowSparse(mat, v, newMat, idx, op);\n" +
		  "  } else { // sorted\n" +
		  "MatrixCommonFuncs.getInstance().onRowSorted(mat, v, newMat, idx, op);\n" +
		  "  }\n" +
		  "  return newMat;\n" +
		  "}\n" +
		  "}\n\n";
  private static String matrixVsDummyVectorByIndex = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "if (onCol && op.isInplace()) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColDummy(mat, v, mat, idx, op);\n" +
		  "  return mat;\n" +
		  "} else if (onCol && !op.isInplace()) {\n" +
		  "  &dtype1Low[] newData = ArrayCopy.copy(data);\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  MatrixCommonFuncs.getInstance().onColDummy(mat, v, newMat, idx, op);\n" +
		  "  return newMat;\n" +
		  "} else if (!onCol && op.isInplace()) {\n" +
		  "  MatrixCommonFuncs.getInstance().onRowDummy(mat, v, mat, idx, op);\n" +
		  "  return mat;\n" +
		  "} else {\n" +
		  "  &dtype1Low[] newData = ArrayCopy.copy(data);\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  MatrixCommonFuncs.getInstance().onRowDummy(mat, v, newMat, idx, op);\n" +
		  "  return newMat;\n" +
		  "}\n" +
		  "}\n\n";
  private static String matrixVsVector = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "if (onCol && op.isInplace()) {\n" +
		  "  if (v.isDense()) {\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColDense(mat, v, mat, j, op);\n" +
		  "}\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColSparse(mat, v, mat, j, op);\n" +
		  "}\n" +
		  "  } else { // sorted\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColSorted(mat, v, mat, j, op);\n" +
		  "}\n" +
		  "  }\n" +
		  "  return mat;\n" +
		  "} else if (onCol && !op.isInplace()) {\n" +
		  "  &dtype1Low[] newData;\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION) {\n" +
		  "newData = new &dtype1Low[m * n];\n" +
		  "  } else {\n" +
		  "newData = ArrayCopy.copy(data);\n" +
		  "  }\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  if (v.isDense()) {\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColDense(mat, v, newMat, j, op);\n" +
		  "}\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColSparse(mat, v, newMat, j, op);\n" +
		  "}\n" +
		  "  } else { // sorted\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onColSorted(mat, v, newMat, j, op);\n" +
		  "}\n" +
		  "  }\n" +
		  "  return newMat;\n" +
		  "} else if (!onCol && op.isInplace()) {\n" +
		  "  if (v.isDense()) {\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  " MatrixCommonFuncs.getInstance().onRowDense(mat, v, mat, i, op);\n" +
		  "}\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onRowSparse(mat, v, mat, i, op);\n" +
		  "}\n" +
		  "  } else { // sorted\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onRowSorted(mat, v, mat, i, op);\n" +
		  "}\n" +
		  "  }\n" +
		  "  return mat;\n" +
		  "} else {\n" +
		  "  &dtype1Low[] newData;\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION) {\n" +
		  "newData = new &dtype1Low[m * n];\n" +
		  "  } else {\n" +
		  "newData = ArrayCopy.copy(data);\n" +
		  "  }\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  if (v.isDense()) {\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onRowDense(mat, v, newMat, i, op);\n" +
		  "}\n" +
		  "  } else if (v.isSparse()) {\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onRowSparse(mat, v, newMat, i, op);\n" +
		  "}\n" +
		  "  } else { // sorted\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  MatrixCommonFuncs.getInstance().onRowSorted(mat, v, newMat, i, op);\n" +
		  "}\n" +
		  "  }\n" +
		  "  return newMat;\n" +
		  "}\n" +
		  "}\n\n";
  private static String matrixVsDummyVector = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "if (onCol && op.isInplace()) {\n" +
		  "  for (int j = 0; j < n; j++) {\n" +
		  "MatrixCommonFuncs.getInstance().onColDummy(mat, v, mat, j, op);\n" +
		  "  }\n" +
		  "  return mat;\n" +
		  "} else if (onCol && !op.isInplace()) {\n" +
		  "  &dtype1Low[] newData;\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION) {\n" +
		  "newData = new &dtype1Low[m * n];\n" +
		  "  } else {\n" +
		  "newData = ArrayCopy.copy(data);\n" +
		  "  }\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  " \n" +
		  "  for (int j = 0; j < n; j++) {\n" +
		  "MatrixCommonFuncs.getInstance().onColDummy(mat, v, newMat, j, op);\n" +
		  "  }\n" +
		  "  return newMat;\n" +
		  "} else if (!onCol && op.isInplace()) {\n" +
		  "  for (int i = 0; i < n; i++) {\n" +
		  "MatrixCommonFuncs.getInstance().onRowDummy(mat, v, mat, i, op);\n" +
		  "  }\n" +
		  "  return mat;\n" +
		  "} else {\n" +
		  "  &dtype1Low[] newData;\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION) {\n" +
		  "newData = new &dtype1Low[m * n];\n" +
		  "  } else {\n" +
		  "newData = ArrayCopy.copy(data);\n" +
		  "  }\n" +
		  "  Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat.getMatrixId(), mat.getClock(), m, n, newData);\n" +
		  "  \n" +
		  "  for (int i = 0; i < n; i++) {\n" +
		  "MatrixCommonFuncs.getInstance().onRowDummy(mat, v, newMat, i, op);\n" +
		  "  }\n" +
		  "  return newMat;\n" +
		  "}\n" +
		  "  }\n\n";
  private static String matrixVsMatrix = "{\n" +
		  "int m = mat1.getNumRows(), n = mat1.getNumCols();\n" +
		  "int size = m * n;\n" +
		  "if (trans1 && trans2) { // TT\n" +
		  "  return MatrixCommonFuncs.getInstance().transVsTrans(mat1, mat2, op);\n" +
		  "} else if (!trans1 && trans2) { // _T\n" +
		  "  if (op.isInplace()) {\n" +
		  "return MatrixCommonFuncs.getInstance().notTransVsTrans(mat1, mat2, mat1, op);\n" +
		  "  } else {\n" +
		  "&dtype1Low[] newData = new &dtype1Low[size];\n" +
		  "Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);\n" +
		  "return MatrixCommonFuncs.getInstance().notTransVsTrans(mat1, mat2, newMat, op);\n" +
		  "  }\n" +
		  "} else if (trans1 && !trans2) { //T_\n" +
		  "  return MatrixCommonFuncs.getInstance().transVsNotTrans(mat1, mat2, op);\n" +
		  "} else {\n" +
		  "  if (op.isInplace()) {\n" +
		  "return MatrixCommonFuncs.getInstance().notTransVsNotTrans(mat1, mat2, mat1, op);\n" +
		  "  } else {\n" +
		  "&dtype1Low[] newData = new &dtype1Low[size];\n" +
		  "Blas&dtype1Matrix newMat = new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), m, n, newData);\n" +
		  "return MatrixCommonFuncs.getInstance().notTransVsNotTrans(mat1, mat2, newMat, op);\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  public static Matrix apply(Matrix mat, Vector v, int idx, boolean onCol, Binary op) {
	return getInstance().apply(mat, v, idx, onCol, op);
  }

  public static Matrix apply(Matrix mat, Vector v, boolean onCol, Binary op) {
	return getInstance().apply(mat, v, onCol, op);
  }

  public static Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2, Binary op) {
	return getInstance().apply(mat1, trans1, mat2, trans2, op);
  }

  private static MatrixExecutor getInstance() {
	if (executor == null) {
	  synchronized (MatrixExecutor.class) {
		if (executor == null) {
		  try {
			String[] dtypes1 = new String[]{"Double", "Float"};
			String[] dtypes2 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String[] names = new String[]{"matrixVsVectorByIndex", "matrixVsVector", "matrixVsMatrix",
					"matrixVsDummyVectorByIndex", "matrixVsDummyVector"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.matrix.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.tencent.angel.ml.math2.matrix.*;\n" +
					"import com.tencent.angel.ml.math2.matrix.Matrix;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.OpType;\n" +
					"import com.tencent.angel.ml.math2.utils.ArrayCopy;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n" +
					"import com.sun.tools.javac.util.Assert;\n\n" +
					"Matrix apply(Matrix mat, Unary op) {\n" +
					"	throw new AngelException(\"The operation is not supported!\");\n" +
					"}\n\n" +
					"public Matrix apply(Matrix mat, Vector v, int idx, boolean onCol, Binary op) {\n" +
					"    if (mat instanceof BlasDoubleMatrix && v instanceof IntDoubleVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntDoubleVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntFloatVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntFloatVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntLongVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntLongVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntIntVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntIntVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntDummyVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntDummyVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntFloatVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntFloatVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntLongVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntLongVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntIntVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntIntVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntDummyVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntDummyVector) v, idx, onCol, op);\n" +
					"    } else if (mat instanceof RowBasedMatrix) {\n" +
					"      Assert.check(!onCol);\n" +
					"      return ((RowBasedMatrix) mat).calulate(idx, v, op);\n" +
					"    } else {\n" +
					"      throw new AngelException(\"The operation is not supported!\");\n" +
					"    }" +
					"}\n\n" +
					"public Matrix apply(Matrix mat, Vector v, boolean onCol, Binary op) {\n" +
					"    if (mat instanceof BlasDoubleMatrix && v instanceof IntDoubleVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntDoubleVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntFloatVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntFloatVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntLongVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntLongVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntIntVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntIntVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasDoubleMatrix && v instanceof IntDummyVector) {\n" +
					"      return apply((BlasDoubleMatrix) mat, (IntDummyVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntFloatVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntFloatVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntLongVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntLongVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntIntVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntIntVector) v, onCol, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix && v instanceof IntDummyVector) {\n" +
					"      return apply((BlasFloatMatrix) mat, (IntDummyVector) v, onCol, op);\n" +
					"    } else if (mat instanceof RowBasedMatrix) {\n" +
					"      Assert.check(!onCol);\n" +
					"      return ((RowBasedMatrix) mat).calulate(v, op);\n" +
					"    } else {\n" +
					"      throw new AngelException(\"The operation is not supported!\");\n" +
					"    }\n" +
					"  }\n\n" +
					" public Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2, Binary op) {\n" +
					"    if (mat1 instanceof BlasMatrix && mat2 instanceof BlasMatrix) {\n" +
					"      if (trans1 == trans2) {\n" +
					"        Assert.check(((BlasMatrix) mat1).getNumRows() == ((BlasMatrix) mat2).getNumRows());\n" +
					"        Assert.check(((BlasMatrix) mat1).getNumCols() == ((BlasMatrix) mat2).getNumCols());\n" +
					"      } else {\n" +
					"        Assert.check(((BlasMatrix) mat1).getNumRows() == ((BlasMatrix) mat2).getNumCols());\n" +
					"        Assert.check(((BlasMatrix) mat1).getNumCols() == ((BlasMatrix) mat2).getNumRows());\n" +
					"      }\n" +
					"    } else if (mat1 instanceof RowBasedMatrix && mat2 instanceof RowBasedMatrix) {\n" +
					"      Assert.check(!trans1 && !trans2);\n" +
					"      int numRow1 = ((RowBasedMatrix) mat1).getRows().length;\n" +
					"      int numRow2 = ((RowBasedMatrix) mat2).getRows().length;\n" +
					"      Assert.check(numRow1 == numRow2);\n" +
					"      Assert.check(((RowBasedMatrix) mat1).getDim() == ((RowBasedMatrix) mat2).getDim());\n" +
					"    } else {\n" +
					"      throw new AngelException(\"The operation is not supported!\");\n" +
					"    }\n" +
					"    if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof BlasDoubleMatrix) {\n" +
					"      return apply((BlasDoubleMatrix) mat1, trans1, (BlasDoubleMatrix) mat2, trans2, op);\n" +
					"    } else if (mat1 instanceof BlasDoubleMatrix && mat2 instanceof BlasFloatMatrix) {\n" +
					"      return apply((BlasDoubleMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2, op);\n" +
					"    } else if (mat1 instanceof BlasFloatMatrix && mat2 instanceof BlasFloatMatrix) {\n" +
					"      return apply((BlasFloatMatrix) mat1, trans1, (BlasFloatMatrix) mat2, trans2, op);\n" +
					"    } else if (mat1 instanceof RowBasedMatrix) {\n" +
					"      return ((RowBasedMatrix) mat1).calulate(mat2, op);\n" +
					"    } else {\n" +
					"      throw new AngelException(\"The operation is not supported!\");\n" +
					"    }\n" +
					"  }\n\n";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int k = 0; k < names.length; k++) {
			  for (int i = 0; i < dtypes1.length; i++) {
				for (int j = i; j < dtypes2.length; j++) {
				  if (names[k] == "matrixVsDummyVectorByIndex" && dtypes2[j] == "Dummy") {
					declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, Int&dtype2Vector v, " +
									"int idx, boolean onCol, Binary op");
					body = matrixVsDummyVectorByIndex;
				  } else if (names[k] == "matrixVsDummyVector" && dtypes2[j] == "Dummy") {
					declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, Int&dtype2Vector v, " +
									"boolean onCol, Binary op");
					body = matrixVsDummyVector;
				  } else if (names[k] == "matrixVsVectorByIndex" && dtypes2[j] != "Dummy") {
					declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, Int&dtype2Vector v, " +
									"int idx, boolean onCol, Binary op");
					body = matrixVsVectorByIndex;
				  } else if (names[k] == "matrixVsVector" && dtypes2[j] != "Dummy") {
					declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, Int&dtype2Vector v, " +
									"boolean onCol, Binary op");
					body = matrixVsVector;
				  } else if (names[k] == "matrixVsMatrix" && (dtypes2[j] == "Double" || dtypes2[j] == "Float")) {
					declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat1, boolean trans1, " +
									"Blas&dtype2Matrix mat2, boolean trans2, Binary op");
					body = matrixVsMatrix;
				  } else {
					declare = "";
					body = "";
				  }

				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[i].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[i])
						  .replaceAll("&dtype2", dtypes2[j]);
				  template += method;
				}
			  }
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (MatrixExecutor) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					MatrixExecutor.class,
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
