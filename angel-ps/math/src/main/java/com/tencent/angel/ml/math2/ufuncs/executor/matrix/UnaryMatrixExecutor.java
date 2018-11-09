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


package com.tencent.angel.ml.math2.ufuncs.executor.matrix;

import com.tencent.angel.ml.math2.matrix.Matrix;
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;

public class UnaryMatrixExecutor {
  private volatile static MatrixExecutor executor;
  private static String unary = "{\n" +
		  "&dtypeLow[] data = mat.getData();\n" +
		  "int size = mat.getNumRows() * mat.getNumCols();\n" +
		  "if (op.isInplace()) {\n" +
		  "	for (int i = 0; i < size; i++) {\n" +
		  "		data[i] = op.apply(data[i]);\n" +
		  "	}\n" +
		  "	return mat;\n" +
		  "} else {\n" +
		  "	&dtypeLow[] newData = new &dtypeLow[size];\n" +
		  "	for (int i = 0; i < size; i++) {\n" +
		  "		newData[i] = op.apply(data[i]);\n" +
		  "	}\n" +
		  "	return new Blas&dtypeMatrix(mat.getMatrixId(), mat.getClock(), mat.getNumRows(),\n" +
		  "	mat.getNumCols(), newData);\n" +
		  "}\n" +
		  "}\n\n";

  public static Matrix apply(Matrix mat, Unary op) {
	Matrix m = getInstance().apply(mat, op);
	return m;
  }

  private static MatrixExecutor getInstance() {
	if (executor == null) {
	  synchronized (MatrixExecutor.class) {
		if (executor == null) {
		  try {
			String[] dtypes = new String[]{"Double", "Float"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor.matrix.*;\n" +
					"import com.tencent.angel.ml.math2.exceptions.MathException;\n" +
					"import com.tencent.angel.ml.math2.matrix.*;\n" +
					"import com.tencent.angel.ml.math2.matrix.Matrix;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.utils.ArrayCopy;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n" +
					"public Matrix apply(Matrix mat, Unary op) {\n" +
					"	if (mat instanceof BlasDoubleMatrix) {\n" +
					"      return apply((BlasDoubleMatrix) mat, op);\n" +
					"    } else if (mat instanceof BlasFloatMatrix) {\n" +
					"      return apply((BlasFloatMatrix) mat, op);\n" +
					"    } else if (mat instanceof RowBasedMatrix) {\n" +
					"      return ((RowBasedMatrix) mat).calulate(op);\n" +
					"    } else {\n" +
					"      throw new MathException(\"The operation is not supported!\");\n" +
					"    }\n" +
					"}\n\n" +
					"public Matrix apply(Matrix mat, Vector v, int idx, boolean onCol, Binary op) {\n" +
					"   throw new MathException(\"The operation is not supported!\");\n" +
					"}\n\n" +
					"public Matrix apply(Matrix mat, Vector v, boolean onCol, Binary op) {\n" +
					"   throw new MathException(\"The operation is not supported!\");\n" +
					"  }\n\n" +
					" public Matrix apply(Matrix mat1, boolean trans1, Matrix mat2, boolean trans2, Binary op) {\n" +
					"   throw new MathException(\"The operation is not supported!\");\n" +
					"  }\n\n";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < dtypes.length; i++) {
			  declare = declTemplate.replaceAll("&ReturnType", "Matrix")
					  .replaceAll("&Parameters", "Blas&dtypeMatrix mat, Unary op");
			  body = unary;

			  method = bodyTemplate.replaceAll("&Declaration", declare)
					  .replaceAll("&Body", body)
					  .replaceAll("&dtypeLow", dtypes[i].toLowerCase())
					  .replaceAll("&dtype", dtypes[i]);
			  template += method;
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (MatrixExecutor) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					"UnaryMatrixExecutor",
					null,
					new Class[]{MatrixExecutor.class},
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
