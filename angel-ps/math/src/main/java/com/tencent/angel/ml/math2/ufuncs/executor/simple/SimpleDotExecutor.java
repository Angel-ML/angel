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
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;

public class SimpleDotExecutor {
  private volatile static Executor executor;
  private static String DummyVsDummy = "{\n" +
		  "	Assert.check(v1.getDim() == v2.getDim());\n" +
		  " int dot = 0;\n" +
		  " &itypeLow[] keys1 = v1.getIndices();\n" +
		  " &itypeLow[] keys2 = v2.getIndices();\n" +
		  " int v1Pointor = 0;\n" +
		  " int v2Pointor = 0;\n" +
		  " while (v1Pointor < keys1.length && v2Pointor < keys2.length) {\n" +
		  " 	if (keys1[v1Pointor] == keys2[v2Pointor]) {\n" +
		  "        dot += 1;\n" +
		  "        v2Pointor++;\n" +
		  "        v1Pointor++;\n" +
		  "      } else if (keys1[v1Pointor] > keys2[v2Pointor]) {\n" +
		  "        v2Pointor++;\n" +
		  "      } else {\n" +
		  "        v1Pointor++;\n" +
		  "      }\n" +
		  "    }\n" +
		  "    return dot;\n" +
		  "}\n\n";
  private static String OtherVsDummy = "{\n" +
		  "Assert.check(v1.getDim() == v2.getDim());\n" +
		  "&dtype1Low dot = 0;\n" +
		  "&itypeLow[] idxs = v2.getIndices();\n" +
		  "for (int i = 0; i < idxs.length; i++) {\n" +
		  "	dot += v1.get(idxs[i]);\n" +
		  "}\n" +
		  "return dot;\n" +
		  "}\n\n";
  private static String DenseVsDense =
		  "		&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
				  " 	&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
				  " 	int length = v1Values.length;\n" +
				  " 	for (int i = 0; i < length; i++) {\n" +
				  "    		dot += v1Values[i] * v2Values[i];\n" +
				  " 	}\n";
  private static String SparseVsOther =
		  "		ObjectIterator iter = v1.getStorage().entryIterator();\n" +
				  "		while (iter.hasNext()) {\n" +
				  "			&itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter.next();\n" +
				  "         dot += entry.get&dtype1Value() * v2.get(entry.get&itypeKey());\n" +
				  "     }\n";
  private static String OtherVsSparse =
		  "		ObjectIterator iter = v2.getStorage().entryIterator();\n" +
				  "     while (iter.hasNext()) {\n" +
				  "     	&itype2&dtype2Map.Entry entry = (&itype2&dtype2Map.Entry)iter.next();\n" +
				  "         dot += entry.get&dtype2Value() * v1.get(entry.get&itypeKey());\n" +
				  "     }\n";
  private static String OtherVsSorted =
		  "     &itypeLow[] keys = v2.getStorage().getIndices();\n" +
				  "     &dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
				  "     &itypeLow size = v2.size();\n" +
				  "     for (int i = 0; i < size; i++) {\n" +
				  "         dot += v2Values[i] * v1.get(keys[i]);\n" +
				  "     }\n";
  private static String SortedVsOther =
		  "     &itypeLow[] keys = v1.getStorage().getIndices();\n" +
				  "     &dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
				  "     &itypeLow size = v1.size();\n" +
				  "     for (int i = 0; i < size; i++) {\n" +
				  "         dot += v1Values[i] * v2.get(keys[i]);\n" +
				  "     }\n";
  private static String SortedVsSorted =
		  "      &itypeLow[] keys1 = v1.getStorage().getIndices();\n" +
				  "      &dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
				  "      &itypeLow[] keys2 = v2.getStorage().getIndices();\n" +
				  "      &dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
				  "      int v1Pointor = 0;\n" +
				  "      int v2Pointor = 0;\n" +
				  "      &itypeLow size1 = v1.size();\n" +
				  "      &itypeLow size2 = v2.size();\n" +
				  "      while (v1Pointor < size1 && v2Pointor < size2) {\n" +
				  "        if (keys1[v1Pointor] == keys2[v2Pointor]) {\n" +
				  "          dot += v1Values[v1Pointor] * v2Values[v2Pointor];\n" +
				  "          v2Pointor++;\n" +
				  "          v1Pointor++;\n" +
				  "        } else if (keys1[v1Pointor] > keys2[v2Pointor]) {\n" +
				  "          v2Pointor++;\n" +
				  "        } else {\n" +
				  "          v1Pointor++;\n" +
				  "        }\n" +
				  "      }\n";
  private static String IntKey = "{\n" +
		  "&dtype1Low dot = 0;\n" +
		  "if (v1.isDense() && v2.isDense()) {\n" + DenseVsDense +
		  "} else if (v1.isDense() && v2.isSparse()) {\n" + OtherVsSparse +
		  "} else if (v1.isDense() && v2.isSorted()) {\n" + OtherVsSorted +
		  "} else if (v1.isSparse() && v2.isDense()) {\n" + SparseVsOther +
		  "} else if (v1.isSorted() && v2.isDense()) {\n" + SortedVsOther +
		  "} else ";
  private static String LongKey =
		  "if (v1.isSparse() && v2.isSparse()) {\n" +
				  "  if (v1.size() < v2.size()) {\n" + SparseVsOther +
				  "  } else {\n" + OtherVsSparse +
				  "  }\n" +
				  "} else if (v1.isSparse() && v2.isSorted()) {\n" +
				  "  if (v1.size() < v2.size()) {\n" + SparseVsOther +
				  "  } else {\n" + OtherVsSorted +
				  "  }\n" +
				  "} else if (v1.isSorted() && v2.isSparse()) {\n" +
				  "  if (v1.size() < v2.size()) {\n" + SortedVsOther +
				  "  } else {\n" + OtherVsSparse +
				  "  }\n" +
				  "} else if (v1.isSorted() && v2.isSorted()) {\n" + SortedVsSorted +
				  "} else {\n" +
				  "	throw new MathException(\"the operation is not support!\");\n" +
				  "}\n" +
				  "return dot;\n" +
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
			String[] dtypes1 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String[] dtypes2 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor.*;\n" +
					"import com.tencent.angel.ml.math2.exceptions.MathException;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import com.sun.tools.javac.util.Assert;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n" +
					"public Vector apply(Vector v1, Vector v2, Binary op){\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public Vector apply(Vector v, Unary op) {\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public double apply(Vector v1, Vector v2){\n" +
					"	if (v1 instanceof IntDoubleVector && v2 instanceof IntDoubleVector) {\n" +
					"      return apply((IntDoubleVector) v1, (IntDoubleVector) v2);\n" +
					"    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntFloatVector) {\n" +
					"      return apply((IntDoubleVector) v1, (IntFloatVector) v2);\n" +
					"    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntLongVector) {\n" +
					"      return apply((IntDoubleVector) v1, (IntLongVector) v2);\n" +
					"    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((IntDoubleVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof IntDoubleVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((IntDoubleVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof IntFloatVector && v2 instanceof IntFloatVector) {\n" +
					"      return apply((IntFloatVector) v1, (IntFloatVector) v2);\n" +
					"    } else if (v1 instanceof IntFloatVector && v2 instanceof IntLongVector) {\n" +
					"      return apply((IntFloatVector) v1, (IntLongVector) v2);\n" +
					"    } else if (v1 instanceof IntFloatVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((IntFloatVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof IntFloatVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((IntFloatVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof IntLongVector && v2 instanceof IntLongVector) {\n" +
					"      return apply((IntLongVector) v1, (IntLongVector) v2);\n" +
					"    } else if (v1 instanceof IntLongVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((IntLongVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof IntLongVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((IntLongVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof IntIntVector && v2 instanceof IntIntVector) {\n" +
					"      return apply((IntIntVector) v1, (IntIntVector) v2);\n" +
					"    } else if (v1 instanceof IntIntVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((IntIntVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof IntDummyVector && v2 instanceof IntDummyVector) {\n" +
					"      return apply((IntDummyVector) v1, (IntDummyVector) v2);\n" +
					"    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongDoubleVector) {\n" +
					"      return apply((LongDoubleVector) v1, (LongDoubleVector) v2);\n" +
					"    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongFloatVector) {\n" +
					"      return apply((LongDoubleVector) v1, (LongFloatVector) v2);\n" +
					"    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongLongVector) {\n" +
					"      return apply((LongDoubleVector) v1, (LongLongVector) v2);\n" +
					"    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((LongDoubleVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof LongDoubleVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((LongDoubleVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof LongFloatVector && v2 instanceof LongFloatVector) {\n" +
					"      return apply((LongFloatVector) v1, (LongFloatVector) v2);\n" +
					"    } else if (v1 instanceof LongFloatVector && v2 instanceof LongLongVector) {\n" +
					"      return apply((LongFloatVector) v1, (LongLongVector) v2);\n" +
					"    } else if (v1 instanceof LongFloatVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((LongFloatVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof LongFloatVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((LongFloatVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof LongLongVector && v2 instanceof LongLongVector) {\n" +
					"      return apply((LongLongVector) v1, (LongLongVector) v2);\n" +
					"    } else if (v1 instanceof LongLongVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((LongLongVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof LongLongVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((LongLongVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof LongIntVector && v2 instanceof LongIntVector) {\n" +
					"      return apply((LongIntVector) v1, (LongIntVector) v2);\n" +
					"    } else if (v1 instanceof LongIntVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((LongIntVector) v1, (LongDummyVector) v2);\n" +
					"    } else if (v1 instanceof LongDummyVector && v2 instanceof LongDummyVector) {\n" +
					"      return apply((LongDummyVector) v1, (LongDummyVector) v2);\n" +
					"    } else {\n" +
					"      throw new MathException(\"Vector type is not support!\");\n" +
					"    }\n" +
					"}\n\n";


			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes1.length; j++) {
				for (int k = j; k < dtypes2.length; k++) {
				  declare = declTemplate.replaceAll("&ReturnType", "&dtype1Low")
						  .replaceAll("&Parameters", "&itype&dtype1Vector v1, &itype&dtype2Vector v2");
				  if (dtypes1[j] == "Dummy" && dtypes1[j] == dtypes2[k]) {
					declare = declTemplate.replaceAll("&ReturnType", "int")
							.replaceAll("&Parameters", "&itype&dtype1Vector v1, &itype&dtype2Vector v2");
					body = DummyVsDummy;
				  } else if (dtypes2[k] == "Dummy") {
					body = OtherVsDummy;
				  } else if (dtypes1[j] == "Dummy") {
					continue;
				  } else {
					if (itypes[i] == "Int") {
					  body = IntKey + LongKey;
					} else {
					  body = "{\n" +
							  "&dtype1Low dot = 0;\n" +
							  LongKey;
					}
				  }
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&itypeLow", itypes[i].toLowerCase())
						  .replaceAll("&dtype1Low", dtypes1[j].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes2[k].toLowerCase())
						  .replaceAll("&itype", itypes[i])
						  .replaceAll("&dtype1", dtypes1[j])
						  .replaceAll("&dtype2", dtypes2[k]);
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
