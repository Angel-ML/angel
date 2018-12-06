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


package com.tencent.angel.ml.math2.ufuncs.executor.simple;

import com.tencent.angel.ml.math2.ufuncs.executor.Executor;
import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;


public class SimpleUnaryExecutor {
  private volatile static Executor executor;
  private static String LongKeyUnary = "{\n" +
		  "&itype&dtypeVector res;\n" +
		  "if (op.isOrigin() || v1.isDense()) {\n" +
		  "	if (!op.isInplace()) {\n" +
		  "		res = v1.copy();\n" +
		  "	} else {\n" +
		  "		res = v1;\n" +
		  "	}\n" +
		  "	if (v1.isDense()) {\n" +
		  "		&dtypeLow[] values = res.getStorage().getValues();\n" +
		  "		for (int i = 0; i < values.length; i++) {\n" +
		  "			values[i] = op.apply(values[i]);\n" +
		  "		}\n" +
		  "	} else if (v1.isSparse()) {\n" +
		  "		ObjectIterator iter = res.getStorage().entryIterator();\n" +
		  "		while (iter.hasNext()) {\n" +
		  "			&itype2&dtypeMap.Entry entry = (&itype2&dtypeMap.Entry)iter.next();\n" +
		  "			entry.setValue(op.apply(entry.get&dtypeValue()));\n" +
		  "		}\n" +
		  "	} else if (v1.isSorted()) {\n" +
		  "		&dtypeLow[] values = res.getStorage().getValues();\n" +
		  "		for (int i = 0; i < v1.size(); i++) {\n" +
		  "			values[i] = op.apply(values[i]);\n" +
		  "		}\n" +
		  "	} else {\n" +
		  "		throw new MathException(\"The operation is not support!\");\n" +
		  "	}\n" +
		  "} else {\n";
  private static String IntKeyUnary =
		  "	&itype&dtypeVectorStorage newstorage = v1.getStorage().emptyDense();\n" +
				  "	&itype&dtypeVectorStorage storage = v1.getStorage();\n" +
				  "	&dtypeLow[] values = newstorage.getValues();\n" +
				  "	&dtypeLow tmp = op.apply((&dtypeLow) 0);\n" +
				  "	&itypeLow dim = v1.getDim();\n" +
				  "	for (int i = 0; i < dim; i++) {\n" +
				  "		values[i] = tmp;\n" +
				  "	}\n" +
				  "	if (v1.isSparse()) {\n" +
				  "		ObjectIterator iter = storage.entryIterator();\n" +
				  "		while (iter.hasNext()) {\n" +
				  "			&itype2&dtypeMap.Entry entry = (&itype2&dtypeMap.Entry)iter.next();\n" +
				  "			values[entry.get&itypeKey()] = op.apply(entry.get&dtypeValue());\n" +
				  "		}\n" +
				  "	} else { //sort\n" +
				  "		&itypeLow[] idxs = storage.getIndices();\n" +
				  "		&dtypeLow[] v1Values = storage.getValues();\n" +
				  "		for (int k = 0; k < idxs.length; k++) {\n" +
				  "			values[idxs[k]] = op.apply(v1Values[k]);\n" +
				  "		}\n" +
				  "	}\n" +
				  "	if (op.isInplace()) {\n" +
				  "		v1.setStorage(newstorage);\n" +
				  "		res = v1;\n" +
				  "	} else {\n" +
				  "		res = new &itype&dtypeVector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(),newstorage);\n" +
				  "	}\n" +
				  "}\n" +
				  "return res;\n" +
				  "}\n\n";

  public static Vector apply(Vector v1, Unary op) {
	Vector v = getInstance().apply(v1, op);
	return v;
  }

  private static Executor getInstance() {
	if (executor == null) {
	  synchronized (Executor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor.*;\n" +
					"import com.tencent.angel.ml.math2.exceptions.MathException;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n" +
					"public Vector apply(Vector v1, Vector v2, Binary op){\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public double apply(Vector v1, Vector v2){\n" +
					"  throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public Vector apply(Vector v, Unary op) {\n" +
					"	if (v instanceof IntDoubleVector) {\n" +
					"      return apply((IntDoubleVector) v, op);\n" +
					"    } else if (v instanceof IntFloatVector) {\n" +
					"      return apply((IntFloatVector) v, op);\n" +
					"    } else if (v instanceof IntLongVector) {\n" +
					"      return apply((IntLongVector) v, op);\n" +
					"    } else if (v instanceof IntIntVector) {\n" +
					"      return apply((IntIntVector) v, op);\n" +
					"    } else if (v instanceof LongDoubleVector) {\n" +
					"      return apply((LongDoubleVector) v, op);\n" +
					"    } else if (v instanceof LongFloatVector) {\n" +
					"      return apply((LongFloatVector) v, op);\n" +
					"    } else if (v instanceof LongLongVector) {\n" +
					"      return apply((LongLongVector) v, op);\n" +
					"    } else if (v instanceof LongIntVector) {\n" +
					"      return apply((LongIntVector) v, op);\n" +
					"    } else {\n" +
					"      throw new MathException(\"Vector type is not support!\");\n" +
					"    }\n" +
					"}\n\n";


			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes.length; j++) {
				declare = declTemplate.replaceAll("&ReturnType", "Vector")
						.replaceAll("&Parameters", "&itype&dtypeVector v1, Unary op")
						.replaceAll("&itpye", itypes[i])
						.replaceAll("&dtype1", dtypes[j]);
				if (itypes[i] == "Int") {
				  body = LongKeyUnary + IntKeyUnary;
				} else {
				  body = LongKeyUnary +
						  " throw new MathException(\"The operation is not supported!\");\n" +
						  "    }\n" +
						  "    return res;\n" +
						  "}\n\n";
				}

				method = bodyTemplate.replaceAll("&Declaration", declare)
						.replaceAll("&Body", body)
						.replaceAll("&itypeLow", itypes[i].toLowerCase())
						.replaceAll("&dtypeLow", dtypes[j].toLowerCase())
						.replaceAll("&itype", itypes[i])
						.replaceAll("&dtype", dtypes[j]);
				template += method;
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
