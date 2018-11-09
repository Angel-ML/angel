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

import com.tencent.angel.ml.math2.utils.ForkJoinUtils;
import com.tencent.angel.ml.math2.utils.UnionEle;
import com.tencent.angel.ml.math2.vector.ComponentVector;
import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.RecursiveTask;

public class CompReduceExecutor {
  private static final int THREADS = ForkJoinUtils.getNCores();
  private volatile static CompExecutor executor;
  private static ForkJoinPool pool = ForkJoinUtils.getPool();
  private static String reduce = "{\n" +
		  "UnionEle res = new UnionEle();\n" +
		  "&itype&dtypeVector[] parts = v.getPartitions();\n" +
		  "if (op == ReduceOP.Sum) {\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setDouble1(res.getDouble1() + parts[i].sum());\n" +
		  "	}\n" +
		  "}else if (op == ReduceOP.Avg){\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setDouble1(res.getDouble1() + parts[i].sum());\n" +
		  "		res.setLong1(res.getLong1() + parts[i].getDim());\n" +
		  "	}\n" +
		  "}else if(op == ReduceOP.Std){\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setDouble1(res.getDouble1() + parts[i].sum());\n" +
		  "		double norm = parts[i].norm();\n" +
		  "		res.setDouble2(res.getDouble2() + norm * norm);\n" +
		  "		res.setLong1(res.getLong1() + parts[i].getDim());\n" +
		  "	}\n" +
		  "}else if(op == ReduceOP.Norm){\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		double norm = parts[i].norm();\n" +
		  "		res.setDouble2(res.getDouble2() + norm * norm);\n" +
		  "	}\n" +
		  "}else if(op == ReduceOP.Min){\n" +
		  "	res.setDouble1(Double.MAX_VALUE);\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setDouble1(Math.min(res.getDouble1(), parts[i].min()));\n" +
		  "	}\n" +
		  "}else if(op == ReduceOP.Max){\n" +
		  "	res.setDouble1(Double.MIN_VALUE);\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setDouble1(Math.max(res.getDouble1(), parts[i].max()));\n" +
		  "	}\n" +
		  "}else if (op == ReduceOP.Size){\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setLong1(res.getLong1() + parts[i].size());\n" +
		  "	}\n" +
		  "}else if(op == ReduceOP.Numzeros){\n" +
		  "	for (int i = start; i <= end; i++) {\n" +
		  "		res.setLong1(res.getLong1() + parts[i].numZeros());\n" +
		  "	}\n" +
		  "}\n" +
		  "return res;\n" +
		  "}\n\n";

  public static double apply(ComponentVector vector, ReduceOP op) {
	CompRedExe task = new CompRedExe(vector, op, 0, vector.getNumPartitions() - 1);
	Future<UnionEle> futRes = pool.submit(task);

	try {
	  UnionEle tmpRes = futRes.get();
	  switch (op) {
		case Avg:
		  return tmpRes.getDouble1() / tmpRes.getLong1();
		case Std:
		  double avg1 = tmpRes.getDouble1() / tmpRes.getLong1();
		  double avg2 = tmpRes.getDouble2() / tmpRes.getLong1();
		  return Math.sqrt(avg2 - avg1 * avg1);
		case Norm:
		  return Math.sqrt(tmpRes.getDouble2());
		default:
		  return tmpRes.getDouble1() + tmpRes.getLong1();
	  }
	} catch (InterruptedException | ExecutionException e) {
	  e.printStackTrace();
	}

	return Double.NaN;
  }

  private static UnionEle merge(UnionEle left, UnionEle right, ReduceOP op) {
	UnionEle res = new UnionEle();
	switch (op) {
	  case Min:
		res.setDouble1(Math.min(left.getDouble1(), right.getDouble1()));
		res.setDouble2(Math.min(left.getDouble2(), right.getDouble2()));
		res.setFloat1(Math.min(left.getFloat1(), right.getFloat1()));
		res.setFloat2(Math.min(left.getFloat2(), right.getFloat2()));
		res.setLong1(Math.min(left.getLong1(), right.getLong1()));
		res.setLong2(Math.min(left.getLong2(), right.getLong2()));
		res.setInt1(Math.min(left.getInt1(), right.getInt1()));
		res.setInt2(Math.min(left.getInt2(), right.getInt2()));
		break;
	  case Max:
		res.setDouble1(Math.max(left.getDouble1(), right.getDouble1()));
		res.setDouble2(Math.max(left.getDouble2(), right.getDouble2()));
		res.setFloat1(Math.max(left.getFloat1(), right.getFloat1()));
		res.setFloat2(Math.max(left.getFloat2(), right.getFloat2()));
		res.setLong1(Math.max(left.getLong1(), right.getLong1()));
		res.setLong2(Math.max(left.getLong2(), right.getLong2()));
		res.setInt1(Math.max(left.getInt1(), right.getInt1()));
		res.setInt2(Math.max(left.getInt2(), right.getInt2()));
		break;
	  default:
		res.setDouble1(left.getDouble1() + right.getDouble1());
		res.setDouble2(left.getDouble2() + right.getDouble2());
		res.setFloat1(left.getFloat1() + right.getFloat1());
		res.setFloat2(left.getFloat2() + right.getFloat2());
		res.setLong1(left.getLong1() + right.getLong1());
		res.setLong2(left.getLong2() + right.getLong2());
		res.setInt1(left.getInt1() + right.getInt1());
		res.setInt2(left.getInt2() + right.getInt2());
	}

	res.setBool(left.isBool() && right.isBool());

	return res;
  }

  public static CompExecutor getInstance() {
	if (executor == null) {
	  synchronized (CompExecutor.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
			String template =
					"import com.tencent.angel.ml.math2.exceptions.MathException;\n" +
							"import com.tencent.angel.ml.math2.utils.UnionEle;\n" +
							"import com.tencent.angel.ml.math2.vector.*;\n" +
							"import com.tencent.angel.ml.math2.ufuncs.expression.*;\n" +
							"import java.util.concurrent.ExecutionException;\n" +
							"import com.tencent.angel.ml.math2.ufuncs.executor.comp.CompReduceExecutor.*;\n\n" +
							"public void apply (ComponentVector v1, ComponentVector v2, Binary op, ComponentVector result, int start, int end){\n" +
							"	throw new MathException(\"Vector type is not support!\");\n" +
							"}\n\n" +
							"public double apply (ComponentVector v1, ComponentVector v2, int start, int end){\n" +
							"	throw new MathException(\"Vector type is not support!\");\n" +
							"}\n\n" +
							"public void apply (ComponentVector v, Unary op, ComponentVector result, int start, int end){\n" +
							"	throw new MathException(\"Vector type is not support!\");\n" +
							"}\n\n" +
							"public UnionEle apply (ComponentVector v, ReduceOP op, int start, int end){\n" +
							"	if (v instanceof CompIntDoubleVector) {\n" +
							"		return apply((CompIntDoubleVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompIntFloatVector) {\n" +
							"		 return apply((CompIntFloatVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompIntLongVector) {\n" +
							"		return apply((CompIntLongVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompIntIntVector) {\n" +
							"		return apply((CompIntIntVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompLongDoubleVector) {\n" +
							"		return apply((CompLongDoubleVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompLongFloatVector) {\n" +
							"		return apply((CompLongFloatVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompLongLongVector) {\n" +
							"		return apply((CompLongLongVector) v, op, start, end);\n" +
							"	} else if (v instanceof CompLongIntVector) {\n" +
							"		return apply((CompLongIntVector) v, op, start, end);\n" +
							"	} else {\n" +
							"		throw new MathException(\"\");\n" +
							"	}\n" +
							"}\n\n";


			String declTemplate = "public &ReturnType apply(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare, body, method;
			for (int i = 0; i < itypes.length; i++) {
			  for (int j = 0; j < dtypes.length; j++) {
				declare = declTemplate.replaceAll("&ReturnType", "UnionEle")
						.replaceAll("&Parameters", "Comp&itype&dtypeVector v, ReduceOP op, int start, int end")
						.replaceAll("&itpye", itypes[i])
						.replaceAll("&dtype1", dtypes[j]);
				body = reduce;

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

  public enum ReduceOP {
	Sum, Avg, Std, Norm, Min, Max, Size, Numzeros
  }

  private static class CompRedExe extends RecursiveTask<UnionEle> {
	private ComponentVector v;
	private ReduceOP op;
	private int start, end, threshold;

	public CompRedExe(ComponentVector v, ReduceOP op, int start, int end) {
	  assert v != null && op != null;
	  this.v = v;
	  this.op = op;
	  this.start = start;
	  this.end = end;
	  this.threshold = (v.getNumPartitions() + THREADS - 1) / THREADS;
	}


	@Override
	protected UnionEle compute() {
	  boolean canCompute = (end - start) < threshold;

	  if (canCompute) {
		return getInstance().apply(v, op, start, end);
	  } else {
		int middle = (start + end) >> 1;

		CompRedExe left = new CompRedExe(v, op, start, middle);
		CompRedExe right = new CompRedExe(v, op, middle + 1, end);

		left.fork();
		right.fork();

		UnionEle resLeft = left.join();
		UnionEle resRight = right.join();

		return merge(resLeft, resRight, op);
	  }
	}
  }
}
