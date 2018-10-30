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

import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;

public class MatrixCommonFuncs {
  private volatile static MatrixUtilsFuncs executor;

  private static String replace = "{\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "if (op.getOpType() == OpType.INTERSECTION){\n" +
		  "  if (onCol && op.isInplace()){\n" +
		  "		for (int i = 0; i < m; i++) {\n" +
		  " 	  if (flag[i] == 0) {\n" +
		  " 		data[i * n + idx] = 0;\n" +
		  " 	  }\n" +
		  " 	}\n" +
		  "  } else if (!onCol && op.isInplace()){\n" +
		  " 	for (int j = 0; j < n; j++) {\n" +
		  " 	  if (flag[j] == 0) {\n" +
		  " 		data[idx * n + j] = 0;\n" +
		  " 	  }\n" +
		  " 	}\n" +
		  "  }\n" +
		  "} else if (op.getOpType() == OpType.ALL) {\n" +
		  "  if (onCol && op.isInplace()){\n" +
		  " 	for (int i = 0; i < m; i++) {\n" +
		  " 	  if (flag[i] == 0) {\n" +
		  " 		data[i * n + idx] = op.apply(data[i * n + idx], 0);\n" +
		  " 	  }\n" +
		  " 	}\n" +
		  "  } else if (!onCol && op.isInplace()){\n" +
		  " 	for (int j = 0; j < n; j++) {\n" +
		  " 	  if (flag[j] == 0) {\n" +
		  " 		data[idx * n + j] = op.apply(data[idx * n + j], 0);\n" +
		  " 	  }\n" +
		  " 	}\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String onColDense = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "&dtype2Low[] values = v.getStorage().getValues();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  resData[i * n + idx] = op.apply(data[i * n + idx], values[i]);\n" +
		  "}\n" +
		  "}\n\n";

  private static String onColSparse = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData = resMat.getData();\n" +
		  "byte[] flag = new byte[v.getDim()];\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "ObjectIterator iter = v.getStorage().entryIterator();\n" +
		  "while (iter.hasNext()) {\n" +
		  "  Int2&dtype2Map.Entry entry = (Int2&dtype2Map.Entry)iter.next();\n" +
		  "  int i = entry.getIntKey();\n" +
		  "  flag[i] = 1;\n" +
		  "  resData[i * n + idx] = op.apply(data[i * n + idx], entry.get&dtype2Value());\n" +
		  "}\n" +
		  "replace(resMat, idx, true, flag, op);\n" +
		  "}\n\n";

  private static String onColSorted = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "byte[] flag = new byte[v.getDim()];\n" +
		  "int size = v.size();\n" +
		  "int m = mat.getNumRows(), n = mat.getNumCols();\n" +
		  "int[] idxs = v.getStorage().getIndices();\n" +
		  "&dtype2Low[] values = v.getStorage().getValues();\n" +
		  "for (int j = 0; j < size; j++) {\n" +
		  "  int i = idxs[j];\n" +
		  "  flag[i] = 1;\n" +
		  "  resData[i * n + idx] = op.apply(data[i * n + idx], values[j]);\n" +
		  "}\n" +
		  "replace(resMat, idx, true, flag, op);\n" +
		  "}\n\n";

  private static String onRowDense = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "&dtype2Low[] values = v.getStorage().getValues();\n" +
		  "int n = mat.getNumCols();\n" +
		  "for (int j = 0; j < n; j++) {\n" +
		  "  resData[idx * n + j] = op.apply(data[idx * n + j], values[j]);\n" +
		  "}\n" +
		  "}\n\n";


  private static String onRowSparse = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "int n = mat.getNumCols();\n" +
		  "byte[] flag = new byte[v.getDim()];\n" +
		  "ObjectIterator iter = v.getStorage().entryIterator();\n" +
		  "while (iter.hasNext()) {\n" +
		  "  Int2&dtype2Map.Entry entry = (Int2&dtype2Map.Entry)iter.next();\n" +
		  "  int j = entry.getIntKey();\n" +
		  "  flag[j] = 1;\n" +
		  "  resData[idx * n + j] = op.apply(data[idx * n + j], entry.get&dtype2Value());\n" +
		  "}\n" +
		  "replace(resMat, idx, false, flag, op);\n" +
		  "}\n\n";

  private static String onRowSorted = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "int[] idxs = v.getStorage().getIndices();\n" +
		  "&dtype2Low[] values = v.getStorage().getValues();\n" +
		  "int n = mat.getNumCols();\n" +
		  "byte[] flag = new byte[v.getDim()];\n" +
		  "int size = v.size();\n" +
		  "for (int k = 0; k < size; k++) {\n" +
		  "  int j = idxs[k];\n" +
		  "  flag[j] = 1;\n" +
		  "  resData[idx * n + j] = op.apply(data[idx * n + j], values[k]);\n" +
		  "}\n" +
		  "replace(resMat, idx, false, flag, op);\n" +
		  "}\n\n";

  private static String onColDummy = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "int[] idxs = v.getIndices();\n" +
		  "int n = mat.getNumCols();\n" +
		  "byte[] flag = new byte[v.getDim()];\n" +
		  "int size = v.size();\n" +
		  "for (int k = 0; k < size; k++) {\n" +
		  "  int i = idxs[k];\n" +
		  "  flag[i] = 1;\n" +
		  "  resData[i * n + idx] = op.apply(data[i * n + idx], 1);\n" +
		  "}\n" +
		  "replace(resMat, idx, false, flag, op);\n" +
		  "}\n\n";

  private static String onRowDummy = "{\n" +
		  "&dtype1Low[] data = mat.getData();\n" +
		  "&dtype1Low[] resData =  resMat.getData();\n" +
		  "int[] idxs = v.getIndices();\n" +
		  "int n = mat.getNumCols();\n" +
		  "byte[] flag = new byte[v.getDim()];\n" +
		  "int size = v.size();\n" +
		  "for (int k = 0; k < size; k++) {\n" +
		  "  int j = idxs[k];\n" +
		  "  flag[j] = 1;\n" +
		  "  resData[idx * n + j] = op.apply(data[idx * n + j], 1);\n" +
		  "}\n" +
		  "replace(resMat, idx, false, flag, op);\n" +
		  "}\n\n";

  private static String transVsTrans = "{\n" +
		  "&dtype1Low[] mat1Data = mat1.getData();\n" +
		  "&dtype2Low[] mat2Data = mat2.getData();\n" +
		  "int m = mat1.getNumRows(), n = mat1.getNumCols();\n" +
		  "int q = mat2.getNumCols();\n" +
		  "int size = m * n;\n" +
		  "&dtype1Low[] newData = new &dtype1Low[size];\n" +
		  "for (int i = 0; i < n; i++) {\n" +
		  "  for (int j = 0; j < m; j++) {\n" +
		  " 	newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[j * q + i]);\n" +
		  "  }\n" +
		  "}\n" +
		  "return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), n, m, newData);\n" +
		  "}\n\n";

  private static String transVsNotTrans = "{\n" +
		  "&dtype1Low[] mat1Data = mat1.getData();\n" +
		  "&dtype2Low[] mat2Data = mat2.getData();\n" +
		  "int m = mat1.getNumRows(), n = mat1.getNumCols();\n" +
		  "int q = mat2.getNumCols();\n" +
		  "int size = m * n;\n" +
		  "&dtype1Low[] newData = new &dtype1Low[size];\n" +
		  "for (int i = 0; i < n; i++) {\n" +
		  "  for (int j = 0; j < m; j++) {\n" +
		  "		newData[i * m + j] = op.apply(mat1Data[j * n + i], mat2Data[i * q + j]);\n" +
		  "  }\n" +
		  "}\n" +
		  "return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), n, m, newData);\n" +
		  "}\n\n";

  private static String notTransVsTrans = "{\n" +
		  "&dtype1Low[] mat1Data = mat1.getData();\n" +
		  "&dtype2Low[] mat2Data = mat2.getData();\n" +
		  "&dtype1Low[] resData = resMat.getData();\n" +
		  "int m = mat1.getNumRows(), n = mat1.getNumCols();\n" +
		  "int q = mat2.getNumCols();\n" +
		  "for (int i = 0; i < m; i++) {\n" +
		  "  for (int j = 0; j < n; j++) {\n" +
		  " 	resData[i * n + j] = op.apply(mat1Data[i * n + j], mat2Data[j * q + i]);\n" +
		  "  }\n" +
		  "}\n" +
		  "return resMat;\n" +
		  "}\n\n";

  private static String notTransVsNotTrans = "{\n" +
		  "&dtype1Low[] mat1Data = mat1.getData();\n" +
		  "&dtype2Low[] mat2Data = mat2.getData();\n" +
		  "&dtype1Low[] resData = resMat.getData();\n" +
		  "int m = mat1.getNumRows(), n = mat1.getNumCols();\n" +
		  "int size = m * n;\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  "  resData[i] = op.apply(mat1Data[i], mat2Data[i]);\n" +
		  "}\n" +
		  "return resMat;\n" +
		  "}\n\n";

  private static String matrixTransSparse = "{\n" +
		  "&dtype1Low[] data = mat.getData();" +
		  "int c = mat.getNumCols();" +
		  "for (int j = 0; j < c; j++) {\n" +
		  "	ObjectIterator iter = v.getStorage().entryIterator();\n" +
		  "	while (iter.hasNext()) {\n" +
		  "		Int2&dtype2Map.Entry entry = (Int2&dtype2Map.Entry)iter.next();\n" +
		  "		int i = entry.getIntKey();\n" +
		  "		resArr[j] += data[i * c + j] * entry.get&dtype2Value();\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";

  private static String matrixNotTransSparse = "{\n" +
		  "&dtype1Low[] data = mat.getData();" +
		  "int r = mat.getNumRows(), c = mat.getNumCols();" +
		  "for (int i = 0; i < r; i++) {\n" +
		  "	ObjectIterator iter = v.getStorage().entryIterator();\n" +
		  "	while (iter.hasNext()) {\n" +
		  "		Int2&dtype2Map.Entry entry = (Int2&dtype2Map.Entry)iter.next();\n" +
		  "		int j = entry.getIntKey();\n" +
		  "		resArr[i] += data[i * c + j] * entry.get&dtype2Value();\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";

  private static String matrixTransSorted = "{\n" +
		  "&dtype1Low[] data = mat.getData();" +
		  "int r = mat.getNumRows(), c = mat.getNumCols();" +
		  "for (int j = 0; j < r; j++) {\n" +
		  "	int[] idxs = v.getStorage().getIndices();\n" +
		  "	&dtype2Low[] vals = v.getStorage().getValues();\n" +
		  "	for (int k = 0; k < idxs.length; k++) {\n" +
		  "		resArr[j] += data[idxs[k] * c + j] * vals[k];\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";

  private static String matrixNotTransSorted = "{\n" +
		  "&dtype1Low[] data = mat.getData();" +
		  "int r = mat.getNumRows(), c = mat.getNumCols();" +
		  "for (int i = 0; i < r; i++) {\n" +
		  "	int[] idxs = v.getStorage().getIndices();\n" +
		  "	&dtype2Low[] vals = v.getStorage().getValues();\n" +
		  "	for (int k = 0; k < idxs.length; k++) {\n" +
		  "		resArr[i] += data[i * c + idxs[k]] * vals[k];\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";


  private static String blasVsBlas = "{\n" +
		  "&dtype1Low alpha = 1, beta = 0;\n" +
		  "int m = mat1.getNumRows(), n = mat1.getNumCols();\n" +
		  "int p = mat2.getNumRows(), q = mat2.getNumCols();\n" +
		  "&dtype1Low[] resBlas;\n" +
		  "if (trans1 && trans2) { // M1^T * M2^T\n" +
		  "	if(flag){\n" +
		  "		throw new AngelException(\"RBMatrix is not support to transpose\");\n" +
		  "	} else {\n" +
		  "		Assert.check(m == q);\n" +
		  "		resBlas = new &dtype1Low[n * p];\n" +
		  "		&replace(\"T\", \"T\", p, n, m, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, p);\n" +
		  "		return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), n, p, resBlas);\n" +
		  "	}\n" +
		  "} else if (!trans1 && trans2) { // M1 * M2^T\n" +
		  "	if(flag){\n" +
		  "		throw new AngelException(\"RBMatrix is not support to transpose\");\n" +
		  "	} else {\n" +
		  "		Assert.check(n == q);\n" +
		  "		resBlas = new &dtype1Low[m * p];\n" +
		  " 	&replace(\"T\", \"N\", p, m, n, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, p);\n" +
		  "		return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), m, p, resBlas);\n" +
		  "	}\n" +
		  "} else if (trans1 && !trans2) { // M1^T * M2\n" +
		  "	Assert.check(m == p);\n" +
		  "	resBlas = new &dtype1Low[n * q];\n" +
		  "	&replace(\"N\", \"T\", q, n, m, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, q);\n" +
		  "	return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), n, q, resBlas);\n" +
		  "} else { // M1 * M2\n" +
		  "	Assert.check(n == p);\n" +
		  "	resBlas = new &dtype1Low[m * q];\n" +
		  "	&replace(\"N\", \"N\", q, m, n, alpha, mat2.getData(), q, mat1.getData(), n, beta, resBlas, q);\n" +
		  "	return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), m, q, resBlas);\n" +
		  "}\n" +
		  "}\n\n";

  private static String rbMatrix2BlasMatrix = "{\n" +
		  "int dim = (int) mat1.getDim();\n" +
		  "int subDim = mat1.getSubDim();\n" +
		  "Vector[] rows = mat1.getRows();\n" +
		  "&dtype1Low[] mat1Data = new &dtype1Low[rows.length * dim];\n" +
		  "int rowId = 0;\n" +
		  "for (int i = 0; i<rows.length; i++) {\n" +
		  "	Int&dtype1Vector[] partitions = ((CompInt&dtype1Vector)rows[i]).getPartitions();\n" +
		  "	int partId = 0;\n" +
		  "	for (int j = 0; j <partitions.length; j++) {\n" +
		  "		Int&dtype1Vector part = partitions[j];\n" +
		  "		Assert.check(part.isDense());\n" +
		  "		&dtype1Low[] src = part.getStorage().getValues();\n" +
		  "		System.arraycopy(src, 0, mat1Data, rowId * dim + partId * subDim, src.length);\n" +
		  "		partId += 1;\n" +
		  "	}\n" +
		  "rowId += 1;\n" +
		  "}\n" +
		  "return new Blas&dtype1Matrix(mat1.getMatrixId(), mat1.getClock(), mat1.getNumRows(), (int) mat1.getDim(), mat1Data);\n" +
		  "}\n\n";


  public static MatrixUtilsFuncs getInstance() {
	if (executor == null) {
	  synchronized (MatrixUtilsFuncs.class) {
		if (executor == null) {
		  try {

			String[] dtypes1 = new String[]{"Double", "Float"};
			String[] dtypes2 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String[] names = new String[]{"replace", "onColDense", "onColSparse", "onColSorted", "onRowDense", "onRowSparse",
					"onRowSorted", "onColDummy", "onRowDummy", "transVsTrans", "transVsNotTrans", "notTransVsTrans",
					"notTransVsNotTrans", "matrixTransSparse", "matrixNotTransSparse", "matrixTransSorted",
					"matrixNotTransSorted", "blasVsBlas", "rbMatrix2BlasMatrix"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.matrix.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.sun.tools.javac.util.Assert;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Binary;\n" +
					"import com.tencent.angel.ml.math2.vector.Vector;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import com.tencent.angel.ml.math2.matrix.*;\n" +
					"import com.tencent.angel.ml.math2.utils.ArrayCopy;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.OpType;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.OpType;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import com.github.fommil.netlib.BLAS;\n" +
					"import java.util.*;\n\n" +
					"private static BLAS blas = BLAS.getInstance();\n";

			String declTemplate = "public &ReturnType &Name(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare = "", body = "", method = "";

			for (int i = 0; i < names.length; i++) {
			  for (int j = 0; j < dtypes1.length; j++) {
				if (names[i] == "replace") {
				  declare = declTemplate.replaceAll("&ReturnType", "void")
						  .replaceAll("&Name", names[i])
						  .replaceAll("&Parameters", "Blas&dtype1Matrix mat, " +
								  "int idx, Boolean onCol, byte[] flag, Binary op");
				  body = replace;
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[j]);
				  template += method;
				} else if (names[i] == "rbMatrix2BlasMatrix") {
				  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
						  .replaceAll("&Name", names[i])
						  .replaceAll("&Parameters", "RBCompInt&dtype1Matrix mat1");
				  body = rbMatrix2BlasMatrix;
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[j]);
				  template += method;
				} else if (names[i] == "blasVsBlas") {
				  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
						  .replaceAll("&Name", names[i])
						  .replaceAll("&Parameters", "Blas&dtype1Matrix mat1, boolean trans1, " +
								  "Blas&dtype2Matrix mat2, boolean trans2, boolean flag");
				  if (dtypes1[j] == "Double") {
					body = blasVsBlas.replaceAll("&replace", "blas.dgemm");
				  } else {
					body = blasVsBlas.replaceAll("&replace", "blas.sgemm");
				  }
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes1[j].toLowerCase())
						  .replaceAll("&dtype1", dtypes1[j])
						  .replaceAll("&dtype2", dtypes1[j]);
				  template += method;
				}

				for (int p = j; p < dtypes2.length; p++) {
				  if ((dtypes1[j] == "Double" || dtypes1[j] == "Float") && (dtypes2[p] == "Double" || dtypes2[p] == "Float")) {
					declare = declTemplate.replaceAll("&ReturnType", "Matrix")
							.replaceAll("&Name", names[i])
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat1, " +
									"Blas&dtype2Matrix mat2, &res Binary op");

					if (names[i] == "transVsTrans") {
					  declare = declare.replaceAll("&res", "");
					  body = transVsTrans;
					} else if (names[i] == "transVsNotTrans") {
					  declare = declare.replaceAll("&res", "");
					  body = transVsNotTrans;
					} else if (names[i] == "notTransVsTrans") {
					  declare = declare.replaceAll("&res", "Blas&dtype1Matrix resMat,");
					  body = notTransVsTrans;
					} else if (names[i] == "notTransVsNotTrans") {
					  declare = declare.replaceAll("&res", "Blas&dtype1Matrix resMat,");
					  body = notTransVsNotTrans;
					} else {
					  body = "";
					  declare = "";
					}
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
							.replaceAll("&dtype2Low", dtypes2[p].toLowerCase())
							.replaceAll("&dtype1", dtypes1[j])
							.replaceAll("&dtype2", dtypes2[p]);
					template += method;
				  }
				  if (dtypes2[p] != "Dummy") {
					if (names[i].startsWith("matrix")) {
					  declare = declTemplate.replaceAll("&ReturnType", "void")
							  .replaceAll("&Name", names[i])
							  .replaceAll("&Parameters", "Blas&dtype1Matrix mat, " +
									  "Int&dtype2Vector v, &dtype1Low[] resArr");
					  if (names[i] == "matrixTransSparse") {
						body = matrixTransSparse;
					  } else if (names[i] == "matrixNotTransSparse") {
						body = matrixNotTransSparse;
					  } else if (names[i] == "matrixTransSorted") {
						body = matrixTransSorted;
					  } else if (names[i] == "matrixNotTransSorted") {
						body = matrixNotTransSorted;
					  }
					} else {
					  declare = declTemplate.replaceAll("&ReturnType", "void")
							  .replaceAll("&Name", names[i])
							  .replaceAll("&Parameters", "Blas&dtype1Matrix mat, " +
									  "Int&dtype2Vector v, Blas&dtype1Matrix resMat, int idx, Binary op");

					  if (names[i] == "onColDense") {
						body = onColDense;
					  } else if (names[i] == "onColSparse") {
						body = onColSparse;
					  } else if (names[i] == "onColSorted") {
						body = onColSorted;
					  } else if (names[i] == "onRowDense") {
						body = onRowDense;
					  } else if (names[i] == "onRowSparse") {
						body = onRowSparse;
					  } else if (names[i] == "onRowSorted") {
						body = onRowSorted;
					  } else {
						body = "";
						declare = "";
					  }
					}

					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
							.replaceAll("&dtype2Low", dtypes2[p].toLowerCase())
							.replaceAll("&dtype1", dtypes1[j])
							.replaceAll("&dtype2", dtypes2[p]);
					template += method;
				  } else {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[i])
							.replaceAll("&Parameters", "Blas&dtype1Matrix mat, " +
									"Int&dtype2Vector v, Blas&dtype1Matrix resMat, int idx, Binary op")
							.replaceAll("&dtype1", dtypes1[j])
							.replaceAll("&dtype2", dtypes2[p]);
					if (names[i] == "onColDummy") {
					  body = onColDummy;
					} else if (names[i] == "onRowDummy") {
					  body = onRowDummy;
					} else {
					  body = "";
					  declare = "";
					}
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
							.replaceAll("&dtype1", dtypes1[j])
							.replaceAll("&dtype2", dtypes2[p]);
					template += method;
				  }
				}
			  }
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (MatrixUtilsFuncs) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					MatrixUtilsFuncs.class,
					(ClassLoader) null
			);
		  } catch (Exception e) {
			System.out.println(e);
		  }
		}
	  }

	}
	return executor;
  }
}
