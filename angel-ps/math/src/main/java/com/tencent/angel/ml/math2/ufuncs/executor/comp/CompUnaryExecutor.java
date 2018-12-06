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


package com.tencent.angel.ml.math2.ufuncs.executor.comp;

import com.tencent.angel.ml.math2.ufuncs.expression.Unary;
import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.utils.VectorUtils;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class CompUnaryExecutor {
  private static final int THREADS = ForkJoinUtils.getNCores();
  private volatile static CompExecutor executor;
  private static ForkJoinPool pool = ForkJoinUtils.getPool();
  private static String unary = "{\n" +
		  "&itype&dtypeVector[] v1Parts = v.getPartitions();\n" +
		  "if (op.isInplace()) {\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		UnaryExecutor.apply(v1Parts[i], op);\n" +
		  "	}\n" +
		  "} else {\n" +
		  "	&itype&dtypeVector[] resParts = result.getPartitions();\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		resParts[i] = (&itype&dtypeVector) UnaryExecutor.apply(v1Parts[i], op);\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";

  public static Vector apply(ComponentVector v1, Unary op) {
	ComponentVector result;

	if (op.isInplace()) {
	  result = v1;
	} else {
	  result = (ComponentVector) VectorUtils.emptyLike((Vector) v1);
	}

	CompUnaExe task = new CompUnaExe(v1, op, result, 0, v1.getNumPartitions() - 1);
	pool.execute(task);
	task.join();

	return (Vector) result;
  }

  public static CompExecutor getInstance() {
	if (executor == null) {
	  synchronized (CompExecutor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor.UnaryExecutor;\n" +
					"import com.tencent.angel.ml.math2.utils.UnionEle;\n" +
					"import com.tencent.angel.ml.math2.exceptions.MathException;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Unary;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Binary;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor.comp.CompReduceExecutor.*;\n\n" +
					"public void apply (ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result, int start, int end){\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public double apply (ComponentVector v1, ComponentVector v2, int start, int end){\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public UnionEle apply (ComponentVector v, ReduceOP op, int start, int end){\n" +
					"	throw new MathException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public void apply (ComponentVector v, Unary op, ComponentVector result, int start, int end){\n" +
					"	if (v instanceof CompIntDoubleVector) {\n" +
					"		apply((CompIntDoubleVector) v, op, (CompIntDoubleVector) result, start, end);\n" +
					"	} else if (v instanceof CompIntFloatVector) {\n" +
					"		apply((CompIntFloatVector) v, op, (CompIntFloatVector) result, start, end);\n" +
					"	} else if (v instanceof CompIntLongVector) {\n" +
					"		apply((CompIntLongVector) v, op, (CompIntLongVector) result, start, end);\n" +
					"	} else if (v instanceof CompIntIntVector) {\n" +
					"		apply((CompIntIntVector) v, op, (CompIntIntVector) result, start, end);\n" +
					"	} else if (v instanceof CompLongDoubleVector) {\n" +
					"		apply((CompLongDoubleVector) v, op, (CompLongDoubleVector) result, start, end);\n" +
					"	} else if (v instanceof CompLongFloatVector) {\n" +
					"		apply((CompLongFloatVector) v, op, (CompLongFloatVector) result, start, end);\n" +
					"	} else if (v instanceof CompLongLongVector) {\n" +
					"		apply((CompLongLongVector) v, op, (CompLongLongVector) result, start, end);\n" +
					"	} else if (v instanceof CompLongIntVector) {\n" +
					"		apply((CompLongIntVector) v, op, (CompLongIntVector) result, start, end);\n" +
					"	} else {\n" +
					"		throw new MathException(\"Vector type is not support!\");\n" +
					"	}\n" +
					"}\n\n";


			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes.length; j++) {
				declare = declTemplate.replaceAll("&ReturnType", "void")
						.replaceAll("&Parameters", "Comp&itype&dtypeVector v, Unary op, " +
								"Comp&itype&dtypeVector result, int start, int end");
				body = unary;

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
			executor = (CompExecutor) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					CompExecutor.class,
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

  private static class CompUnaExe extends RecursiveAction {
	int start, end, threshold;
	ComponentVector v, result;
	Unary op;

	public CompUnaExe(ComponentVector v, Unary op, ComponentVector result, int start, int end) {
	  assert v != null && op != null;
	  this.start = start;
	  this.end = end;
	  this.threshold = (v.getNumPartitions() + THREADS - 1) / THREADS;
	  ;
	  this.v = v;
	  this.result = result;
	  this.op = op;
	}

	@Override
	protected void compute() {
	  boolean canCompute = (end - start) < threshold;

	  if (canCompute) {
		getInstance().apply(v, op, result, start, end);
	  } else {
		int middle = (start + end) >> 1;

		CompUnaExe left = new CompUnaExe(v, op, result, start, middle);
		CompUnaExe right = new CompUnaExe(v, op, result, middle + 1, end);

		invokeAll(left, right);
	  }
	}
  }
}
