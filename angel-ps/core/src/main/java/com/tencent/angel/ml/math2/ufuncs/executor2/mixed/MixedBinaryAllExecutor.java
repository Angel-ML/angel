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


package com.tencent.angel.ml.math2.ufuncs.executor2.mixed;

import com.tencent.angel.ml.math2.ufuncs.executor2.Executor;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;

public class MixedBinaryAllExecutor {
  private volatile static Executor executor;

  private static String IntKeyFunc = "{\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);\n" +
		  "if (v2.isDense()) {\n" +
		  "  int base = 0, k = 0;\n" +
		  "  for (int i = 0; i< parts.length; i++) {\n" +
		  "		&itype&dtype1Vector part = parts[i];\n" +
		  "		&itype&dtype1VectorStorage resPart = (&itype&dtype1VectorStorage)resParts[k];\n" +
		  "		if (part.isDense()) {\n" +
		  " 		 CommonFuncs.getInstance().CompDenseVsOther(part, v2, resPart, base, op);\n" +
		  "		} else if (part.isSparse()) {\n" +
		  "  		if (part.size() < Constant.denseLoopThreshold * part.getDim()) {\n" +
		  "				CommonFuncs.getInstance().CompLeftZeroOperation(part, v2, resPart, base, op);\n" +
		  "				CommonFuncs.getInstance().CompSparseVsOther(part, v2, resPart, base, op);\n" +
		  "  		} else {\n" +
		  "				CommonFuncs.getInstance().CompOtherVsDense(part, v2, resPart, base, op);\n" +
		  "  		}\n" +
		  "		} else { // sorted\n" +
		  "  		if (part.size() < Constant.denseLoopThreshold * part.getDim()) {\n" +
		  "				CommonFuncs.getInstance().CompLeftZeroOperation(part, v2, resPart, base, op);\n" +
		  "				CommonFuncs.getInstance().CompSortedVsOther1(part, v2, resPart, base, op);\n" +
		  "  		} else {\n" +
		  "				CommonFuncs.getInstance().CompOtherVsDense(part, v2, resPart, base, op);\n" +
		  "  		}\n" +
		  "		}\n" +
		  "		base += part.getDim();\n" +
		  "		k++;\n" +
		  "  }\n" +
		  "	return CommonFuncs.getInstance().CompBuildCompVector(v1, resParts, op);\n" +
		  "} else {\n";

  private static String LongKeyFunc =
		  "	if (!op.isKeepStorage()) {\n" +
				  "		CommonFuncs.getInstance().CompTransStorage(v1, resParts);\n" +
				  " }\n" +
				  " CommonFuncs.getInstance().CompOtherVsSortedAll(v1, v2, resParts, op);\n" +
				  "	return CommonFuncs.getInstance().CompBuildCompVector(v1, resParts, op);\n" +
				  "}\n";

  public static Executor getInstance() {
	if (executor == null) {
	  synchronized (Executor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes1 = new String[]{"Double", "Float", "Long", "Int"};
			String[] dtypes2 = new String[]{"Double", "Float", "Long", "Int", "Dummy"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor.StorageSwitch;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import com.tencent.angel.ml.math2.utils.Constant;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n" +
					"public Vector apply(Vector v1, Unary op){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public double apply(Vector v1, Vector v2){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public Vector apply(Vector v1, Vector v2, Binary op) {\n" +
					"if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDoubleVector) {\n" +
					"  return apply((CompIntDoubleVector) v1, (IntDoubleVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntFloatVector) {\n" +
					"  return apply((CompIntDoubleVector) v1, (IntFloatVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntLongVector) {\n" +
					"  return apply((CompIntDoubleVector) v1, (IntLongVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((CompIntDoubleVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntDoubleVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((CompIntDoubleVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntFloatVector && v2 instanceof IntFloatVector) {\n" +
					"  return apply((CompIntFloatVector) v1, (IntFloatVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntFloatVector && v2 instanceof IntLongVector) {\n" +
					"  return apply((CompIntFloatVector) v1, (IntLongVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntFloatVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((CompIntFloatVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntFloatVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((CompIntFloatVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntLongVector && v2 instanceof IntLongVector) {\n" +
					"  return apply((CompIntLongVector) v1, (IntLongVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntLongVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((CompIntLongVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntLongVector && v2 instanceof IntDummyVector) {\n" +
					"  return apply((CompIntLongVector) v1, (IntDummyVector) v2, op);\n" +
					"} else if (v1 instanceof CompIntIntVector && v2 instanceof IntIntVector) {\n" +
					"  return apply((CompIntIntVector) v1, (IntIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDoubleVector) {\n" +
					"  return apply((CompLongDoubleVector) v1, (LongDoubleVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongFloatVector) {\n" +
					"  return apply((CompLongDoubleVector) v1, (LongFloatVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongLongVector) {\n" +
					"  return apply((CompLongDoubleVector) v1, (LongLongVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((CompLongDoubleVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongDoubleVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((CompLongDoubleVector) v1, (LongDummyVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongFloatVector && v2 instanceof LongFloatVector) {\n" +
					"  return apply((CompLongFloatVector) v1, (LongFloatVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongFloatVector && v2 instanceof LongLongVector) {\n" +
					"  return apply((CompLongFloatVector) v1, (LongLongVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongFloatVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((CompLongFloatVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongFloatVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((CompLongFloatVector) v1, (LongDummyVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongLongVector && v2 instanceof LongLongVector) {\n" +
					"  return apply((CompLongLongVector) v1, (LongLongVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongLongVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((CompLongLongVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongLongVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((CompLongLongVector) v1, (LongDummyVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongIntVector && v2 instanceof LongIntVector) {\n" +
					"  return apply((CompLongIntVector) v1, (LongIntVector) v2, op);\n" +
					"} else if (v1 instanceof CompLongIntVector && v2 instanceof LongDummyVector) {\n" +
					"  return apply((CompLongIntVector) v1, (LongDummyVector) v2, op);\n" +
					"} else {\n" +
					"  throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n" +
					"}\n\n";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes1.length; j++) {
				for (int p = j; p < dtypes2.length; p++) {
				  declare = declTemplate.replaceAll("&ReturnType", "Vector")
						  .replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, &itype&dtype2Vector v2, Binary op");
				  if (itypes[i] == "Int" && dtypes2[p] != "Dummy") {
					body = IntKeyFunc + LongKeyFunc + "  }\n\n";
				  } else {
					body = "{\n" +
							"Storage[] resParts = StorageSwitch.applyComp(v1, v2, op);\n" + LongKeyFunc;
				  }
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
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
					"MixedBinaryAllExecutor",
					null,
					new Class[]{Executor.class},
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
