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


package com.tencent.angel.ml.math2.ufuncs.executor2.simple;

import com.tencent.angel.ml.math2.ufuncs.executor2.Executor;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;

public class SimpleBinaryUnionExecutor {
  private volatile static Executor executor;
  private static String IntKeyDummyFunc = "{\n" +
		  "&itpye&dtype1VectorStorage newStorage = (&itpye&dtype1VectorStorage)StorageSwitch.apply(v1, v2, op);\n" +
		  "if (v1.isDense()) {\n" +
		  "  return DenseDummy.getInstance().union(v1, v2, newStorage, op);\n" +
		  "} else ";

  private static String LongKeyDummyFunc =
		  "if (v1.isSparse()) {\n" +
				  "  return SparseDummy.getInstance().union(v1, v2, newStorage, op);\n" +
				  "} else {\n" +
				  "  return SortedDummy.getInstance().union(v1, v2, newStorage, op);\n" +
				  "}\n" +
				  "}\n\n";

  private static String IntKeyFunc = "{\n" +
		  "&itpye&dtype1VectorStorage newStorage = (&itpye&dtype1VectorStorage)StorageSwitch.apply(v1, v2, op);\n" +
		  "if (v1.isDense() && v2.isDense()) {\n" +
		  "  return DenseDense.getInstance().union(v1, v2, newStorage, op);\n" +
		  "} else if (v1.isDense() && v2.isSparse()) {\n" +
		  "  return DenseSparse.getInstance().union(v1, v2, newStorage, op);\n" +
		  "} else if (v1.isDense() && v2.isSorted()) {\n" +
		  "  return DenseSorted.getInstance().union(v1, v2, newStorage, op);\n" +
		  "} else if (v1.isSparse() && v2.isDense()) {\n" +
		  "  return SparseDense.getInstance().union(v1, v2, newStorage, op);\n" +
		  "} else if (v1.isSorted() && v2.isDense()) {\n" +
		  "  return SortedDense.getInstance().union(v1, v2, newStorage, op);\n" +
		  "} else ";

  private static String LongKeyFunc =
		  "if (v1.isSparse() && v2.isSparse()) {\n" +
				  "  return SparseSparse.getInstance().union(v1, v2, newStorage, op);\n" +
				  "} else if (v1.isSparse() && v2.isSorted()) {\n" +
				  "  return SparseSorted.getInstance().union(v1, v2, newStorage, op);\n" +
				  "} else if (v1.isSorted() && v2.isSparse()) {\n" +
				  "  return SortedSparse.getInstance().union(v1, v2, newStorage, op);\n" +
				  "} else if (v1.isSorted() && v2.isSorted()) {\n" +
				  "  return SortedSorted.getInstance().union(v1, v2, newStorage, op);\n" +
				  "} else {\n" +
				  "  throw new AngelException(\"The operation is not support!\");\n" +
				  "}" +
				  "}\n\n";

  public static Executor getInstance() {
	if (executor == null) {
	  synchronized (Executor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor2.simple.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n\n" +
					"public Vector apply(Vector v1, Unary op){\n" +
					"  throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public double apply(Vector v1, Vector v2){\n" +
					"  throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public Vector apply(Vector v1, Vector v2, Binary op) {\n" +
					"if (v1 instanceof IntDoubleVector && v2 instanceof IntDoubleVector) {\n" +
					"  return apply((IntDoubleVector) v1, (IntDoubleVector) v2, op);\n" +
					"} else if (v1 instanceof IntDoubleVector && v2 instanceof IntFloatVector) {\n" +
					"  return apply((IntDoubleVector) v1, (IntFloatVector) v2, op);\n" +
					"} else if (v1 instanceof IntDoubleVector && v2 instanceof IntLongVector) {\n" +
					"  return apply((IntDoubleVector) v1, (IntLongVector) v2, op);\n" +
					"} else if (v1 instanceof IntDoubleVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((IntDoubleVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof IntDoubleVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((IntDoubleVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof IntFloatVector && v2 instanceof IntFloatVector) {\n" +
					"  return apply((IntFloatVector) v1, (IntFloatVector) v2, op);\n" +
					"} else if (v1 instanceof IntFloatVector && v2 instanceof IntLongVector) {\n" +
					"  return apply((IntFloatVector) v1, (IntLongVector) v2, op);\n" +
					"} else if (v1 instanceof IntFloatVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((IntFloatVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof IntFloatVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((IntFloatVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof IntLongVector && v2 instanceof IntLongVector) {\n" +
					"  return apply((IntLongVector) v1, (IntLongVector) v2, op);\n" +
					"} else if (v1 instanceof IntLongVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((IntLongVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof IntLongVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((IntLongVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof IntIntVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((IntIntVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof IntIntVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((IntIntVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof LongDoubleVector && v2 instanceof LongDoubleVector) {\n" +
					"  return apply((LongDoubleVector) v1, (LongDoubleVector) v2, op);\n" +
					"} else if (v1 instanceof LongDoubleVector && v2 instanceof LongFloatVector) {\n" +
					"  return apply((LongDoubleVector) v1, (LongFloatVector) v2, op);\n" +
					"} else if (v1 instanceof LongDoubleVector && v2 instanceof LongLongVector) {\n" +
					"  return apply((LongDoubleVector) v1, (LongLongVector) v2, op);\n" +
					"} else if (v1 instanceof LongDoubleVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((LongDoubleVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof LongDoubleVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((LongDoubleVector) v1, (LongDummyVector) v2, op);\n" +
					"} else if (v1 instanceof LongFloatVector && v2 instanceof LongFloatVector) {\n" +
					"  return apply((LongFloatVector) v1, (LongFloatVector) v2, op);\n" +
					"} else if (v1 instanceof LongFloatVector && v2 instanceof LongLongVector) {\n" +
					"  return apply((LongFloatVector) v1, (LongLongVector) v2, op);\n" +
					"} else if (v1 instanceof LongFloatVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((LongFloatVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof LongFloatVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((LongFloatVector) v1, (LongDummyVector) v2, op);\n" +
					"} else if (v1 instanceof LongLongVector && v2 instanceof LongLongVector) {\n" +
					"  return apply((LongLongVector) v1, (LongLongVector) v2, op);\n" +
					"} else if (v1 instanceof LongLongVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((LongLongVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof LongLongVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((LongLongVector) v1, (LongDummyVector) v2, op);\n" +
					"} else if (v1 instanceof LongIntVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((LongIntVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof LongIntVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((LongIntVector) v1, (LongDummyVector) v2, op);\n" +
					"} else {\n" +
					"  throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n" +
					"}\n\n";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes.length; j++) {
				declare = declTemplate.replaceAll("&ReturnType", "Vector")
						.replaceAll("&Parameters", "&itpye&dtype1Vector v1, &itpyeDummyVector v2, Binary op")
						.replaceAll("&itpye", itypes[i])
						.replaceAll("&dtype1", dtypes[j]);
				if (itypes[i] == "Int") {
				  body = IntKeyDummyFunc + LongKeyDummyFunc;
				} else {
				  body = "{\n" +
						  "&itpye&dtype1VectorStorage newStorage = (&itpye&dtype1VectorStorage)StorageSwitch.apply(v1, v2, op);\n" +
						  LongKeyDummyFunc;
				}
				method = bodyTemplate.replaceAll("&Declaration", declare)
						.replaceAll("&Body", body)
						.replaceAll("&itpye", itypes[i])
						.replaceAll("&dtype1", dtypes[j]);
				template += method;
				for (int p = j; p < dtypes.length; p++) {
				  declare = declTemplate.replaceAll("&ReturnType", "Vector")
						  .replaceAll("&Parameters", "&itpye&dtype1Vector v1, &itpye&dtype2Vector v2, Binary op")
						  .replaceAll("&itpye", itypes[i])
						  .replaceAll("&dtype1", dtypes[j])
						  .replaceAll("&dtype2", dtypes[p]);
				  if (itypes[i] == "Int") {
					body = IntKeyFunc + LongKeyFunc;
				  } else {
					body = "{\n" +
							"&itpye&dtype1VectorStorage newStorage = (&itpye&dtype1VectorStorage)StorageSwitch.apply(v1, v2, op);\n" + LongKeyFunc;
				  }
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&itpye", itypes[i])
						  .replaceAll("&dtype1", dtypes[j])
						  .replaceAll("&dtype2", dtypes[p]);
				  template += method;
				}
			  }
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (Executor) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					Executor.class,
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
