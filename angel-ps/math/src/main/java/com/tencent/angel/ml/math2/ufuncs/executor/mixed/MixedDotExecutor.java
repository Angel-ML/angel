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

import com.tencent.angel.ml.math2.ufuncs.executor.Executor;
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;

public class MixedDotExecutor {
  private volatile static Executor executor;
  private static String CompVsDummy = "{\n" +
		  "&dtype1Low dotValue = 0;\n" +
		  "&itypeLow[] v2Indices = v2.getIndices();\n" +
		  "for (int i = 0; i < v2Indices.length; i++) {\n" +
		  "  dotValue += v1.get(v2Indices[i]);\n" +
		  "}\n" +
		  "return dotValue;\n" +
		  "}\n\n";
  private static String IntKeyCompVsOther = "{\n" +
		  "&dtype1Low dotValue = 0;\n" +
		  "if (v2.isDense()) {\n" +
		  "	int base = 0;\n" +
		  "	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "	&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "	for (int k = 0; k < parts.length; k++) {\n" +
		  "		&itype&dtype1Vector part = parts[k];\n" +
		  "		if (part.isDense()) {\n" +
		  "			&dtype1Low[] partValues = part.getStorage().getValues();\n" +
		  "          for (int i = 0; i < partValues.length; i++) {\n" +
		  "            int idx = base + i;\n" +
		  "            dotValue += partValues[i] * v2Values[idx];\n" +
		  "          }\n" +
		  "		} else if (part.isSparse()) {\n" +
		  "          ObjectIterator iter = part.getStorage().entryIterator();\n" +
		  "          while (iter.hasNext()) {\n" +
		  "            &itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter.next();\n" +
		  "            int idx = base + entry.get&itypeKey();\n" +
		  "            dotValue += entry.get&dtype1Value() * v2Values[idx];\n" +
		  "          }\n" +
		  "		} else { // isSorted\n" +
		  "          &itypeLow[] partIndices = part.getStorage().getIndices();\n" +
		  "          &dtype1Low[] partValues = part.getStorage().getValues();\n" +
		  "          for (int i = 0; i < partIndices.length; i++) {\n" +
		  "            int idx = base + partIndices[i];\n" +
		  "            dotValue += partValues[i] * v2Values[idx];\n" +
		  "          }\n" +
		  "		}\n" +
		  "        base += part.getDim();\n" +
		  "	}\n" +
		  "} else ";
  private static String LongKeyCompVsOther =
		  "if (v2.isSparse()) {\n" +
				  "	ObjectIterator iter = v2.getStorage().entryIterator();\n" +
				  "	while (iter.hasNext()) {\n" +
				  "		&itype2&dtype2Map.Entry entry = (&itype2&dtype2Map.Entry)iter.next();\n" +
				  "		&itypeLow idx = entry.get&itypeKey();\n" +
				  "		dotValue += v1.get(idx) * entry.get&dtype2Value();\n" +
				  "	}\n" +
				  "} else if (v2.isSorted() && v1.size() > v2.size()) { // v2 is sorted\n" +
				  "	&itypeLow[] v2Indices = v2.getStorage().getIndices();\n" +
				  "	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
				  "	for (int i = 0; i < v2Indices.length; i++) {\n" +
				  "		&itypeLow idx = v2Indices[i];\n" +
				  "		dotValue += v1.get(idx) * v2Values[i];\n" +
				  "	}\n" +
				  "} else {\n" +
				  "	int base = 0;\n" +
				  "	&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
				  "	for (int k = 0; k < parts.length; k++) {\n" +
				  "		&itype&dtype1Vector part = parts[k];\n" +
				  "		if (part.isDense()) {\n" +
				  "			&dtype1Low[] partValues = part.getStorage().getValues();\n" +
				  "			for (int i = 0; i < partValues.length; i++) {\n" +
				  "				int idx = base + i;\n" +
				  "            	dotValue += partValues[i] * v2.get(idx);\n" +
				  "			}\n" +
				  "		} else if (part.isSparse()) {\n" +
				  "			ObjectIterator iter = part.getStorage().entryIterator();\n" +
				  "			while (iter.hasNext()) {\n" +
				  "				&itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter.next();\n" +
				  "            	&itypeLow idx = base + entry.get&itypeKey();\n" +
				  "            	dotValue += entry.get&dtype1Value() * v2.get(idx);\n" +
				  "          }\n" +
				  "		} else { // isSorted\n" +
				  "          &itypeLow[] partIndices = part.getStorage().getIndices();\n" +
				  "          &dtype1Low[] partValues = part.getStorage().getValues();\n" +
				  "          for (int i = 0; i < partIndices.length; i++) {\n" +
				  "            	&itypeLow idx = base + partIndices[i];\n" +
				  "            	dotValue += partValues[i] * v2.get(idx);\n" +
				  "          }\n" +
				  "		}\n" +
				  "		base += part.getDim();\n" +
				  "	}\n" +
				  "}\n" +
				  "return dotValue;\n" +
				  "}\n\n";

  public static double apply(Vector v1, Vector v2) {
	double d = getInstance().apply(v1, v2);
	return d;
  }

  private static Executor getInstance() {
	if (executor == null) {
	  synchronized (Executor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes1 = new String[]{"Double", "Float", "Long", "Int"};
			String[] dtypes2 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor.*;\n" +
					"import com.tencent.angel.ml.math2.exceptions.MathException;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n" +
					"public Vector apply(Vector v1, Unary op){\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public Vector apply(Vector v1, Vector v2, Binary op) {\n" +
					"  throw new MathException(\"Vector type is not support!\");\n" +
					"}\n" +
					"public double apply(Vector v1, Vector v2){\n" +
					"	if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDoubleVector) {\n" +
					"      return apply((CompIntDoubleVector) v1, (IntDoubleVector) v2);\n" +
					"    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntFloatVector) {\n" +
					"      return apply((CompIntDoubleVector) v1, (IntFloatVector) v2);\n" +
					"    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntLongVector) {\n" +
					"      return apply((CompIntDoubleVector) v1, (IntLongVector) v2);\n" +
					"    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((CompIntDoubleVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((CompIntDoubleVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntFloatVector) {\n" +
					"      return apply((CompIntFloatVector) v1, (IntFloatVector) v2);\n" +
					"    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntLongVector) {\n" +
					"      return apply((CompIntFloatVector) v1, (IntLongVector) v2);\n" +
					"    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((CompIntFloatVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof CompIntFloatVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((CompIntFloatVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntLongVector) {\n" +
					"      return apply((CompIntLongVector) v1, (IntLongVector) v2);\n" +
					"    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((CompIntLongVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof CompIntLongVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((CompIntLongVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompIntIntVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((CompIntIntVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof CompIntIntVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((CompIntIntVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDoubleVector) {\n" +
					"      return apply((CompLongDoubleVector) v1, (LongDoubleVector) v2);\n" +
					"    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongFloatVector) {\n" +
					"      return apply((CompLongDoubleVector) v1, (LongFloatVector) v2);\n" +
					"    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongLongVector) {\n" +
					"      return apply((CompLongDoubleVector) v1, (LongLongVector) v2);\n" +
					"    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((CompLongDoubleVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((CompLongDoubleVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongFloatVector) {\n" +
					"      return apply((CompLongFloatVector) v1, (LongFloatVector) v2);\n" +
					"    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongLongVector) {\n" +
					"      return apply((CompLongFloatVector) v1, (LongLongVector) v2);\n" +
					"    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((CompLongFloatVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof CompLongFloatVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((CompLongFloatVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongLongVector) {\n" +
					"      return apply((CompLongLongVector) v1, (LongLongVector) v2);\n" +
					"    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((CompLongLongVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof CompLongLongVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((CompLongLongVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof CompLongIntVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((CompLongIntVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof CompLongIntVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((CompLongIntVector) v1, (LongDummyVector) v2);\n" +
					"    } else {\n" +
					"      throw new MathException(\"The operation is not support!\");\n" +
					"    }\n" +
					"  }\n\n";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes1.length; j++) {
				for (int p = j; p < dtypes2.length; p++) {
				  declare = declTemplate.replaceAll("&ReturnType", "double")
						  .replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, &itype&dtype2Vector v2");
				  if (dtypes2[p] == "Dummy") {
					body = CompVsDummy;
				  } else {
					if (itypes[i] == "Int") {
					  body = IntKeyCompVsOther + LongKeyCompVsOther;
					} else {
					  body = "{\n" +
							  "&dtype1Low dotValue = 0;\n" + LongKeyCompVsOther;
					}
				  }

				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&itypeLow", itypes[i].toLowerCase())
						  .replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes2[p].toLowerCase())
						  .replaceAll("&itype", itypes[i])
						  .replaceAll("&dtype1", dtypes1[j])
						  .replaceAll("&dtype2", dtypes2[p]);
				  template += method;
				}
			  }
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (Executor) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					Executor.class,
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
