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


package com.tencent.angel.ml.math2.ufuncs.executor2.comp;

import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

public class CompDotExecutor {
  private static final int THREADS = ForkJoinUtils.getNCores();
  private volatile static CompExecutor executor;
  private static ForkJoinPool pool = ForkJoinUtils.getPool();
  private static String dot = "{\n" +
		  "&dtype1Low dot = 0;\n" +
		  "if (v1.isCompatable(v2)) {\n" +
		  "	&itype&dtype1Vector[] v1Parts = v1.getPartitions();\n" +
		  "	&itype&dtype2Vector[] v2Parts = v2.getPartitions();\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		dot += SimpleDotExecutor.calculate().apply(v1Parts[i], v2Parts[i]);\n" +
		  "	}\n" +
		  "	return dot;\n" +
		  "} else {\n" +
		  "	throw new AngelException(\"Operation is not support!\");\n" +
		  "}\n" +
		  "}\n\n";

  public static double apply(ComponentVector v1, ComponentVector v2) {
	CompDotExe task = new CompDotExe(v1, v2, 0, v1.getNumPartitions() - 1);
	Future<Double> result = pool.submit(task);

	try {
	  return result.get();
	} catch (InterruptedException | ExecutionException e) {
	  e.printStackTrace();
	}

	return Double.NaN;
  }

  public static CompExecutor getInstance() {
	if (executor == null) {
	  synchronized (CompExecutor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.simple.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.tencent.angel.ml.math2.utils.UnionEle;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Unary;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Binary;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor2.comp.CompReduceExecutorClient.*;\n\n" +
					"public void apply (ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result, int start, int end){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public void apply (ComponentVector v, Unary op, ComponentVector result, int start, int end){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public UnionEle apply (ComponentVector v, ReduceOP op, int start, int end){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public double apply (ComponentVector v1, ComponentVector v2, int start, int end){\n" +
					"	double dot = 0;\n" +
					"	if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntDoubleVector) {\n" +
					"		dot = apply((CompIntDoubleVector) v1, (CompIntDoubleVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntFloatVector) {\n" +
					"		dot = apply((CompIntDoubleVector) v1, (CompIntFloatVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntLongVector) {\n" +
					"		dot = apply((CompIntDoubleVector) v1, (CompIntLongVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntIntVector) {\n" +
					"		dot = apply((CompIntDoubleVector) v1, (CompIntIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntFloatVector) {\n" +
					"		dot = apply((CompIntFloatVector) v1, (CompIntFloatVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntLongVector) {\n" +
					"		dot = apply((CompIntFloatVector) v1, (CompIntLongVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntIntVector) {\n" +
					"		dot = apply((CompIntFloatVector) v1, (CompIntIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntLongVector) {\n" +
					"		dot = apply((CompIntLongVector) v1, (CompIntLongVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntIntVector) {\n" +
					"		dot = apply((CompIntLongVector) v1, (CompIntIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompIntIntVector && v2 instanceof CompIntIntVector) {\n" +
					"		dot = apply((CompIntIntVector) v1, (CompIntIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongDoubleVector) {\n" +
					"		dot = apply((CompLongDoubleVector) v1, (CompLongDoubleVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongFloatVector) {\n" +
					"		dot = apply((CompLongDoubleVector) v1, (CompLongFloatVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongLongVector) {\n" +
					"		dot = apply((CompLongDoubleVector) v1, (CompLongLongVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongIntVector) {\n" +
					"		dot = apply((CompLongDoubleVector) v1, (CompLongIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongFloatVector) {\n" +
					"		dot = apply((CompLongFloatVector) v1, (CompLongFloatVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongLongVector) {\n" +
					"		dot = apply((CompLongFloatVector) v1, (CompLongLongVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongIntVector) {\n" +
					"		dot = apply((CompLongFloatVector) v1, (CompLongIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongLongVector) {\n" +
					"		dot = apply((CompLongLongVector) v1, (CompLongLongVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongIntVector) {\n" +
					"		dot = apply((CompLongLongVector) v1, (CompLongIntVector) v2, start, end);\n" +
					"	} else if (v1 instanceof CompLongIntVector && v2 instanceof CompLongIntVector) {\n" +
					"		dot = apply((CompLongIntVector) v1, (CompLongIntVector) v2, start, end);\n" +
					"	} else {\n" +
					"		throw new AngelException(\"The operation is not support!\");\n" +
					"	}\n" +
					"	return dot;" +
					"}\n\n";


			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes.length; j++) {
				for (int p = j; p < dtypes.length; p++) {
				  declare = declTemplate.replaceAll("&ReturnType", "double")
						  .replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, " +
								  "Comp&itype&dtype2Vector v2, int start, int end");
				  body = dot;

				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
						  .replaceAll("&itypeLow", itypes[i].toLowerCase())
						  .replaceAll("&dtype1Low", dtypes[j].toLowerCase())
						  .replaceAll("&dtype2Low", dtypes[p].toLowerCase())
						  .replaceAll("&itype", itypes[i])
						  .replaceAll("&dtype1", dtypes[j])
						  .replaceAll("&dtype2", dtypes[p]);
				  template += method;
				}
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

  private static class CompDotExe extends RecursiveTask<Double> {
	private int start, end, threshold;
	private ComponentVector v1, v2;

	public CompDotExe(ComponentVector v1, ComponentVector v2, int start, int end) {
	  assert v1 != null && v2 != null;
	  this.start = start;
	  this.end = end;
	  this.threshold = (v1.getNumPartitions() + THREADS - 1) / THREADS;
	  this.v1 = v1;
	  this.v2 = v2;
	}


	@Override
	protected Double compute() {
	  double dot = Double.NaN;
	  boolean canCompute = (end - start) < threshold;

	  if (canCompute) {
		CompDotExecutor.getInstance().apply(v1, v2, start, end);
	  } else {
		int middle = (start + end) >> 1;

		CompDotExe left = new CompDotExe(v1, v2, start, middle);
		CompDotExe right = new CompDotExe(v1, v2, middle + 1, end);

		left.fork();
		right.fork();

		double leftRes = left.join();
		double rightRes = right.join();

		dot = leftRes + rightRes;
	  }

	  return dot;
	}
  }
}
