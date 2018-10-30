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

import com.tencent.angel.ml.math2.ufuncs.expression.Binary;
import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.utils.VectorUtils;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import com.tencent.angel.ml.math2.vector.Vector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.RecursiveAction;

public class CompBinaryExecutor {
  private static final int THREADS = ForkJoinUtils.getNCores();
  private volatile static CompExecutor executor;
  private static ForkJoinPool pool = ForkJoinUtils.getPool();
  private static String CompVsComp = "{\n" +
		  "if (v1.isCompatable(v2)) {\n" +
		  "	&itype&dtype1Vector[] v1Parts = v1.getPartitions();\n" +
		  "	&itype&dtype2Vector[] v2Parts = v2.getPartitions();\n" +
		  "	if (op.isInplace()) {\n" +
		  "		for (int i = start; i <= end; i++) {\n" +
		  "			BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);\n" +
		  "		}\n" +
		  "	} else {\n" +
		  " 	&itype&dtype1Vector[] resParts = result.getPartitions();\n" +
		  "		for (int i = start; i <= end; i++) {\n" +
		  "			resParts[i] = (&itype&dtype1Vector) BinaryExecutor.apply(v1Parts[i], v2Parts[i], op);\n" +
		  "		}\n" +
		  "	}\n" +
		  "} else {\n" +
		  "	throw new AngelException(\"Operation is not support!\");\n" +
		  "}\n" +
		  "}\n\n";

  public static Vector apply(ComponentVector v1, ComponentVector v2, Binary op) {
	ComponentVector result;

	if (op.isInplace()) {
	  result = v1;
	} else {
	  result = (ComponentVector) VectorUtils.emptyLike((Vector) v1);
	}

	CompBinExe task = new CompBinExe(v1, v2, op, result, 0, v1.getNumPartitions() - 1);
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
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor2.*;\n" +
					"import com.tencent.angel.exception.AngelException;\n" +
					"import com.tencent.angel.ml.math2.utils.UnionEle;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor.BinaryExecutor;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Binary;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Unary;\n" +
					"import com.tencent.angel.ml.math2.utils.ForkJoinUtils;\n" +
					"import com.tencent.angel.ml.math2.utils.VectorUtils;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import java.util.concurrent.ForkJoinPool;\n" +
					"import java.util.concurrent.RecursiveAction;\n\n" +
					"import com.tencent.angel.ml.math2.ufuncs.executor2.comp.CompReduceExecutor.*;\n\n" +
					"public void apply (ComponentVector v1, Unary op, ComponentVector result, int start, int end){\n" +
					"	throw new AngelException(\"The operation is not support!\");\n" +
					"}\n\n" +
					"public double apply (ComponentVector v1, ComponentVector v2, int start, int end){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public UnionEle apply (ComponentVector v, ReduceOP op, int start, int end){\n" +
					"	throw new AngelException(\"Vector type is not support!\");\n" +
					"}\n\n" +
					"public void apply (ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result, int start, int end){\n" +
					"	if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntDoubleVector) {\n" +
					"		apply((CompIntDoubleVector) v1, (CompIntDoubleVector) v2, op,\n" +
					"            (CompIntDoubleVector) result, start, end);\n" +
					"	} else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntFloatVector) {\n" +
					"		apply((CompIntDoubleVector) v1, (CompIntFloatVector) v2, op, (CompIntDoubleVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntLongVector) {\n" +
					"		apply((CompIntDoubleVector) v1, (CompIntLongVector) v2, op, (CompIntDoubleVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntDoubleVector && v2 instanceof CompIntIntVector) {\n" +
					"		apply((CompIntDoubleVector) v1, (CompIntIntVector) v2, op, (CompIntDoubleVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntFloatVector) {\n" +
					"		apply((CompIntFloatVector) v1, (CompIntFloatVector) v2, op, (CompIntFloatVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntLongVector) {\n" +
					"		apply((CompIntFloatVector) v1, (CompIntLongVector) v2, op, (CompIntFloatVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntFloatVector && v2 instanceof CompIntIntVector) {\n" +
					"		apply((CompIntFloatVector) v1, (CompIntIntVector) v2, op, (CompIntFloatVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntLongVector) {\n" +
					"		apply((CompIntLongVector) v1, (CompIntLongVector) v2, op, (CompIntLongVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntLongVector && v2 instanceof CompIntIntVector) {\n" +
					"		apply((CompIntLongVector) v1, (CompIntIntVector) v2, op, (CompIntLongVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompIntIntVector && v2 instanceof CompIntIntVector) {\n" +
					"		apply((CompIntIntVector) v1, (CompIntIntVector) v2, op, (CompIntIntVector) result, start,\n" +
					"            end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongDoubleVector) {\n" +
					"		apply((CompLongDoubleVector) v1, (CompLongDoubleVector) v2, op,\n" +
					"            (CompLongDoubleVector) result, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongFloatVector) {\n" +
					"		apply((CompLongDoubleVector) v1, (CompLongFloatVector) v2, op,\n" +
					"            (CompLongDoubleVector) result, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongLongVector) {\n" +
					"		apply((CompLongDoubleVector) v1, (CompLongLongVector) v2, op,\n" +
					"            (CompLongDoubleVector) result, start, end);\n" +
					"	} else if (v1 instanceof CompLongDoubleVector && v2 instanceof CompLongIntVector) {\n" +
					"		apply((CompLongDoubleVector) v1, (CompLongIntVector) v2, op,\n" +
					"            (CompLongDoubleVector) result, start, end);\n" +
					"	} else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongFloatVector) {\n" +
					"		apply((CompLongFloatVector) v1, (CompLongFloatVector) v2, op,\n" +
					"            (CompLongFloatVector) result, start, end);\n" +
					"	} else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongLongVector) {\n" +
					"		apply((CompLongFloatVector) v1, (CompLongLongVector) v2, op, (CompLongFloatVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompLongFloatVector && v2 instanceof CompLongIntVector) {\n" +
					"		apply((CompLongFloatVector) v1, (CompLongIntVector) v2, op, (CompLongFloatVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongLongVector) {\n" +
					"		apply((CompLongLongVector) v1, (CompLongLongVector) v2, op, (CompLongLongVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompLongLongVector && v2 instanceof CompLongIntVector) {\n" +
					"		apply((CompLongLongVector) v1, (CompLongIntVector) v2, op, (CompLongLongVector) result,\n" +
					"            start, end);\n" +
					"	} else if (v1 instanceof CompLongIntVector && v2 instanceof CompLongIntVector) {\n" +
					"		apply((CompLongIntVector) v1, (CompLongIntVector) v2, op, (CompLongIntVector) result,\n" +
					"            start, end);\n" +
					"	} else {\n" +
					"		throw new AngelException(\"The operation is not support!\");\n" +
					"	}\n" +
					"}";
			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;

			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes.length; j++) {
				for (int p = j; p < dtypes.length; p++) {
				  declare = declTemplate.replaceAll("&ReturnType", "void")
						  .replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, " +
								  "Comp&itype&dtype2Vector v2, Binary op, Comp&itype&dtype1Vector result, int start, int end");
				  body = CompVsComp;
				  method = bodyTemplate.replaceAll("&Declaration", declare)
						  .replaceAll("&Body", body)
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
			e.printStackTrace();
		  }
		}
	  }
	}
	return executor;
  }

  private static class CompBinExe extends RecursiveAction {
	private ComponentVector v1, v2, result;
	private int start, end, threshold;
	private Binary op;

	public CompBinExe(ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result,
					  int start, int end) {
	  this.v1 = v1;
	  this.v2 = v2;
	  this.result = result;
	  this.start = start;
	  this.end = end;
	  this.threshold = (v1.getNumPartitions() + THREADS - 1) / THREADS;
	  this.op = op;
	}

	@Override
	protected void compute() {
	  boolean canCompute = (end - start) < threshold;

	  if (canCompute) {
		getInstance().apply(v1, v2, op, result, start, end);
	  } else {
		int middle = (start + end) >> 1;

		CompBinExe left = new CompBinExe(v1, v2, op, result, start, middle);
		CompBinExe right = new CompBinExe(v1, v2, op, result, middle + 1, end);

		invokeAll(left, right);
	  }
	}
  }
}
