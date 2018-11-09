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


package com.tencent.angel.ml.math2.ufuncs.executor;

import org.codehaus.janino.ClassBodyEvaluator;
import org.codehaus.janino.Scanner;

import java.io.StringReader;


public class CommonFuncs {
  private volatile static UtilsFuncs executor;
  private static String rightZeroOperation = "{\n" +
		  "&itypeLow size = v1.getDim();\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  "  newStorage.set(i, op.apply(v1.get(i), 0));\n" +
		  "}\n" +
		  "}\n\n";

  private static String leftZeroOperation = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&itypeLow size = v2.getDim();\n" +
		  "if (!newStorage.isSorted()) {\n" +
		  "	for (int i = 0; i < size; i++) {\n" +
		  "		newStorage.set(i, op.apply(0, v2Values[i]));\n" +
		  "	}\n" +
		  "} else {\n" +
		  " &itypeLow[] resIndices = newStorage.getIndices();\n" +
		  "	for (int i = 0; i < size; i++) {\n" +
		  " 	resIndices[i] = i;\n" +
		  "  	resValues[i] = op.apply(0, v2Values[i]);\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";

  private static String replaceOperation = "{\n" +
		  "if (v1.isSparse()){\n" +
		  "  ObjectIterator iter1 = v1.getStorage().entryIterator();\n" +
		  "  while (iter1.hasNext()) {\n" +
		  "		&itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter1.next();\n" +
		  "		&itypeLow idx = entry.get&itypeKey();\n" +
		  "		newStorage.set(idx, entry.get&dtype1Value());\n" +
		  "  }\n" +
		  "}else{\n" +
		  "  &itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "  &dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "  &itypeLow size = v1.size();\n" +
		  "  for (int i = 0; i < size; i++) {\n" +
		  "		&itypeLow idx = v1Indices[i];\n" +
		  "		newStorage.set(idx, v1Values[i]);\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String otherVsDense = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&itype&dtype1VectorStorage v1Storage = v1.getStorage();\n" +
		  "for (int i = 0; i < resValues.length; i++) {\n" +
		  "	if (v1Storage.hasKey(i)) {\n" +
		  "		resValues[i] = op.apply(v1.get(i), v2Values[i]);\n" +
		  "	} else {\n" +
		  "		resValues[i] = op.apply(0, v2Values[i]);\n" +
		  " }\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String otherVsSparse = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "ObjectIterator iter = v2.getStorage().entryIterator();\n" +
		  "&itype&dtype1VectorStorage storage = v1.getStorage();\n" +
		  "while (iter.hasNext()) {\n" +
		  "  &itype2&dtype2Map.Entry entry = (&itype2&dtype2Map.Entry)iter.next();\n" +
		  "  &itypeLow idx = entry.get&itypeKey();\n" +
		  "  newStorage.set(idx, op.apply(v1.get(idx), entry.get&dtype2Value()));\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String otherVsSorted = "{\n" +
		  "&itypeLow[] v2Indices = v2.getStorage().getIndices();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itype&dtype1VectorStorage storage = v1.getStorage();\n" +
		  "&itypeLow size = v2.size();\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  " &itypeLow idx = v2Indices[i];\n" +
		  " if (newStorage.isDense() || newStorage.isSparse()) {\n" +
		  "  	newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));\n" +
		  "	}else if (newStorage.isSorted()){\n" +
		  "  	resValues[i] = op.apply(storage.get(idx), v2Values[i]);\n" +
		  " }\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String denseVsOtherFaster = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itype&dtype1VectorStorage v1Storage = v1.getStorage();\n" +
		  "&itypeLow size = resValues.length;\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  "  if (v2.hasKey(i)) {\n" +
		  "		newStorage.set(i, op.apply(v1Storage.get(i), v2.get(i)));\n" +
		  "  } else {\n" +
		  "		newStorage.set(i, op.apply(v1Storage.get(i), 0));\n" +
		  "  }\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String otherVsDenseFaster = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itype&dtype1VectorStorage storage = v1.getStorage();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&itypeLow size = resValues.length;\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  "	if (storage.hasKey(i)) {\n" +
		  "		if (newStorage.isSparse()){\n" +
		  "			newStorage.set(i, op.apply(v1.get(i), v2Values[i]));\n" +
		  "		} else {\n" +
		  "			resValues[i] = op.apply(v1.get(i), v2Values[i]);\n" +
		  "		}\n" +
		  "	} else {\n" +
		  "		if (newStorage.isSparse()){\n" +
		  "			newStorage.set(i, op.apply(0, v2Values[i]));\n" +
		  "		} else {\n" +
		  "			resValues[i] = op.apply(0, v2Values[i]);\n" +
		  "		}\n" +
		  "	}\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";


  private static String denseVsDense = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&itypeLow length = v1Values.length;\n" +
		  "for (int idx = 0; idx < length; idx++) {\n" +
		  "  resValues[idx] = op.apply(v1Values[idx], v2Values[idx]);\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sparseVsDense = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "ObjectIterator iter = v1.getStorage().entryIterator();\n" +
		  "while (iter.hasNext()) {\n" +
		  "  &itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter.next();\n" +
		  "  &itypeLow idx = entry.get&itypeKey();\n" +
		  " if (newStorage.isSparse()){\n" +
		  "		newStorage.set(idx, op.apply(entry.get&dtype1Value(), v2Values[idx]));\n" +
		  "	} else {\n" +
		  "     resValues[idx] = op.apply(entry.get&dtype1Value(), v2Values[idx]);\n" +
		  " }" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsDense = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&itypeLow size = v1.size();\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  "  &itypeLow idx = v1Indices[i];\n" +
		  "  if (op.getOpType() == OpType.INTERSECTION){\n" +
		  "		resValues[i] = op.apply(v1Values[i], v2Values[idx]);\n" +
		  "  }else{\n" +
		  "		resValues[idx] = op.apply(v1Values[i], v2Values[idx]);\n" +
		  "  }\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsSparse1 = "{\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&itypeLow[] idxiter = v2.getStorage().indexIterator().to&itypeArray();\n" +
		  "&itypeLow[] indices = new &itypeLow[(int)(v1.size() + v2.size())];\n" +
		  "&dtype1Low[] values = new &dtype1Low[indices.length];\n" +
		  "System.arraycopy(v1Indices, 0, indices, 0, (int) v1.size());\n" +
		  "System.arraycopy(idxiter, 0, indices, (int) v1.size(), (int) v2.size());\n" +
		  "Arrays.sort(indices);\n" +
		  "&itypeAVLTreeSet avl = new &itypeAVLTreeSet(indices);\n" +
		  "&itypeBidirectionalIterator iter = avl.iterator();\n" +
		  "int i = 0;\n" +
		  "while (iter.hasNext()) {\n" +
		  "	&itypeLow idx = iter.next&itype();\n" +
		  "	indices[i] = idx;\n" +
		  "	values[i] = op.apply(v1.get(idx), v2.get(idx));\n" +
		  "	i++;\n" +
		  "}\n" +
		  "while (i < indices.length) {\n" +
		  "	indices[i] = 0;\n" +
		  "	i++;\n" +
		  "}\n" +
		  "newStorage = new &itype&dtype1SortedVectorStorage(v1.getDim(), (int) avl.size(), indices, values);\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsSparse2 = "{\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&itype&dtype2VectorStorage storage = v2.getStorage();\n" +
		  "&itypeLow size = v1.size();\n" +
		  "if (!newStorage.isSorted()) {\n" +
		  " for (int i = 0; i < size; i++) {\n" +
		  "  	&itypeLow idx = v1Indices[i];\n" +
		  "  	if (storage.hasKey(idx)) {\n" +
		  " 	  newStorage.set(idx, op.apply(v1Values[i], storage.get(idx)));\n" +
		  " 	}" +
		  " }" +
		  "} else {\n" +
		  " &itypeLow[] resIndices = newStorage.getIndices();" +
		  " for (int i = 0; i < size; i++) {\n" +
		  "  	&itypeLow idx = v1Indices[i];\n" +
		  "  	if (storage.hasKey(idx)) {\n" +
		  " 	  resIndices[i] = idx;\n" +
		  "	      resValues[i] = op.apply(v1Values[i], storage.get(idx));\n" +
		  " 	}" +
		  " }" +
		  "}" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsSorted1 = "{\n" +
		  "int v1Pointor = 0;\n" +
		  "int v2Pointor = 0;\n" +
		  "&itypeLow size1 = v1.size();\n" +
		  "&itypeLow size2 = v2.size();\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&itypeLow[] v2Indices = v2.getStorage().getIndices();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "&itypeLow[] resIndices = newStorage.getIndices();\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "int globalPointor = 0;\n" +
		  "while (v1Pointor < size1 && v2Pointor < size2) {\n" +
		  "  if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {\n" +
		  "		resIndices[globalPointor] = v1Indices[v1Pointor];\n" +
		  "		resValues[globalPointor] = op.apply(v1Values[v1Pointor], v2Values[v2Pointor]);\n" +
		  "		v1Pointor++;\n" +
		  "		v2Pointor++;\n" +
		  "		globalPointor++;\n" +
		  "  } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "  		resIndices[globalPointor] = v1Indices[v1Pointor];\n" +
		  "  		resValues[globalPointor] = op.apply(v1Values[v1Pointor], 0);\n" +
		  "  		globalPointor++;\n" +
		  "		}\n" +
		  "		v1Pointor++;\n" +
		  "  } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "  		resIndices[globalPointor] = v2Indices[v2Pointor];\n" +
		  "  		resValues[globalPointor] = v2Values[v2Pointor];\n" +
		  "  		globalPointor++;\n" +
		  "		}\n" +
		  "		v2Pointor++;\n" +
		  "  }\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsSorted2 = "{\n" +
		  "int v1Pointor = 0;\n" +
		  "int v2Pointor = 0;\n" +
		  "&itypeLow size1 = v1.size();\n" +
		  "&itypeLow size2 = v2.size();\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&itypeLow[] v2Indices = v2.getStorage().getIndices();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "while (v1Pointor < size1 && v2Pointor < size2) {\n" +
		  "  if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {\n" +
		  "		newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], v2Values[v2Pointor]));\n" +
		  "		v1Pointor++;\n" +
		  "		v2Pointor++;\n" +
		  "  } else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "			newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 0));\n" +
		  "		}\n" +
		  "			v1Pointor++;\n" +
		  "  } else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "			newStorage.set(v2Indices[v2Pointor], op.apply(0, v2Values[v2Pointor]));\n" +
		  "		}\n" +
		  "		v2Pointor++;\n" +
		  "  }\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String allSparse = "{\n" +
		  "\t// multi-rehash\n" +
		  "\t&itype&dtype1VectorStorage v1Storage = v1.getStorage();\n" +
		  "\tfor (int i = 0; i < v1.getDim(); i++) {\n" +
		  "\t  if (v1Storage.hasKey(i) && v2.hasKey(i)) {\n" +
		  "\t\tnewStorage.set(i, op.apply(v1.get(i), v2.get(i)));\n" +
		  "\t  } else if (v1Storage.hasKey(i) && !v2.hasKey(i)) {\n" +
		  "\t\tnewStorage.set(i, op.apply(v1.get(i), 0));\n" +
		  "\t  } else if (!v1Storage.hasKey(i) && v2.hasKey(i)) {\n" +
		  "\t\tnewStorage.set(i, op.apply(0, v2.get(i)));\n" +
		  "\t  } else {\n" +
		  "\t\tnewStorage.set(i, op.apply((&dtype1Low)0, 0));\n" +
		  "\t  }\n" +
		  "\t}\n" +
		  "\treturn new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sparseVsOther = "{\n" +
		  "ObjectIterator iter = v1.getStorage().entryIterator();\n" +
		  "while (iter.hasNext()) {\n" +
		  "	&itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter.next();\n" +
		  "	&itypeLow idx = entry.get&itypeKey();\n" +
		  "	if (v2.hasKey(idx)) {\n" +
		  "  	newStorage.set(idx,op.apply(entry.get&dtype1Value(), v2.get(idx)));\n" +
		  "	}\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";


  private static String CompDenseVsOther = "{\n" +
		  "&dtype1Low[] resPartValues = resPartStorage.getValues();\n" +
		  "&dtype1Low[] partValues = part.getStorage().getValues();\n" +
		  "if (v2.isDense()){\n" +
		  "  &dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "  for (int j = 0; j < partValues.length; j++) {\n" +
		  "    resPartValues[j] = op.apply(partValues[j],  v2Values[j + base]);\n" +
		  "  }\n" +
		  "} else {\n" +
		  "  for (int j = 0; j < partValues.length; j++) {\n" +
		  "		if (v2.hasKey(j + base)) {\n" +
		  "  		resPartValues[j] = op.apply(partValues[j], v2.get(j + base));\n" +
		  "		}\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompLeftZeroOperation = "{\n" +
		  "&itypeLow dim = part.getDim();\n" +
		  "if (resPartStorage.isSorted()){\n" +
		  "  &itypeLow[] resIndices = resPartStorage.getIndices();\n" +
		  "  &dtype1Low[] resValues = resPartStorage.getValues();\n" +
		  "  for (int i = 0; i < dim; i++) {\n" +
		  "		resIndices[i] = i;\n" +
		  "		resValues[i] = op.apply(0, v2.get(i + base));\n" +
		  "  }\n" +
		  "	resPartStorage.set(resIndices, resValues);\n" +
		  "}else {\n" +
		  "  for (int i = 0; i <dim ; i++) {\n" +
		  "		resPartStorage.set(i, op.apply(0, v2.get(i + base)));\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompSparseVsOther = "{\n" +
		  "ObjectIterator iter = part.getStorage().entryIterator();\n" +
		  "while (iter.hasNext()) {\n" +
		  "  &itype2&dtype1Map.Entry entry = (&itype2&dtype1Map.Entry)iter.next();\n" +
		  "  &itypeLow idx = entry.get&itypeKey();\n" +
		  "	 resPartStorage.set(idx, op.apply(entry.get&dtype1Value(), v2.get(base + idx)));\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompOtherVsDense = "{\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "if (resPartStorage.isSorted()){\n" +
		  "  &itypeLow[] resPartIndices = resPartStorage.getIndices();\n" +
		  "  &dtype1Low[] resPartValues = resPartStorage.getValues();\n" +
		  "  &itypeLow size = resPartIndices.length;\n" +
		  "  for (int i = 0; i < size; i++) {\n" +
		  "  	resPartIndices[i] = i;\n" +
		  "		if (part.getStorage().hasKey(i)) {\n" +
		  "  		resPartValues[i] = op.apply(part.get(i),  v2.get(i + base));\n" +
		  "		} else {\n" +
		  "  		resPartStorage.set(i, op.apply(0, v2.get(i + base)));\n" +
		  "		}\n" +
		  "  }\n" +
		  "} else {\n" +
		  "  for (int i = 0; i < part.getDim(); i++) {\n" +
		  "		if (part.getStorage().hasKey(i)) {\n" +
		  "  		resPartStorage.set(i, op.apply(part.get(i), v2.get(i + base)));\n" +
		  "		} else {\n" +
		  "  		resPartStorage.set(i, op.apply(0, v2.get(i + base)));\n" +
		  "		}\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompSortedVsOther1 = "{\n" +
		  "&itypeLow[] partIndices = part.getStorage().getIndices();\n" +
		  "&dtype1Low[] partValues = part.getStorage().getValues();\n" +
		  "&itypeLow size = partIndices.length;\n" +
		  "if (!v2.isDense()){\n" +
		  "  for (int i = 0; i < size; i++) {\n" +
		  "		&itypeLow idx = partIndices[i];\n" +
		  "		if (v2.hasKey(idx + base)) {\n" +
		  "  		resPartStorage.set(idx, op.apply(partValues[i], v2.get(idx + base)));\n" +
		  "		}\n" +
		  "  }\n" +
		  "}else {\n" +
		  "  for (int i = 0; i < size; i++) {\n" +
		  "		&itypeLow idx = partIndices[i];\n" +
		  "		resPartStorage.set(idx, op.apply(partValues[i], v2.get(idx + base)));\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";


  private static String CompSortedVsOther = "{\n" +
		  "&itypeLow[] partIndices = part.getStorage().getIndices();\n" +
		  "&dtype1Low[] partValues = part.getStorage().getValues();\n" +
		  "&itypeLow[] resPartIndices = resPartStorage.getIndices();\n" +
		  "&dtype1Low[] resPartValues = resPartStorage.getValues();\n" +
		  "for (int j = 0; j < partIndices.length; j++) {\n" +
		  "  &itypeLow idx = partIndices[j];\n" +
		  "  if (v2.hasKey(idx)) {\n" +
		  "		resPartIndices[j] = idx;\n" +
		  "		resPartValues[j] = op.apply(partValues[j], v2.get(idx + base));\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompTransStorage = "{\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "for (int i = 0; i< parts.length; i++) {\n" +
		  "  if (parts[i].getStorage() instanceof &itype&dtype1SortedVectorStorage){\n" +
		  "		resPartStorages[i] = new &itype&dtype1SparseVectorStorage(parts[i].getDim(),\n" +
		  "		parts[i].getStorage().getIndices(), parts[i].getStorage().getValues());\n" +
		  "  }\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompOtherVsSparse = "{\n" +
		  "&itypeLow subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "ObjectIterator iter = v2.getStorage().entryIterator();\n" +
		  "int i = 0;" +
		  "while (iter.hasNext()) {\n" +
		  "  &itype2&dtype2Map.Entry entry = (&itype2&dtype2Map.Entry)iter.next();\n" +
		  "  &itypeLow gidx = entry.get&itypeKey();\n" +
		  "  int pidx = (int) (gidx / subDim);\n" +
		  "  &itypeLow subidx = gidx % subDim;\n" +
		  "  ((&itype&dtype1VectorStorage)resPartStorages[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), entry.get&dtype2Value()));\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompOtherVsSorted = "{\n" +
		  "&itypeLow subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "&itypeLow[] v2Indices = v2.getStorage().getIndices();\n" +
		  "&dtype2Low[] v2Values = v2.getStorage().getValues();\n" +
		  "int[] indices = new int[parts.length];\n" +
		  "for (int i = 0; i < v2Indices.length; i++) {\n" +
		  "  &itypeLow gidx = v2Indices[i];\n" +
		  "  int pidx = (int) (gidx / subDim);\n" +
		  "  &itypeLow subidx = gidx % subDim;\n" +
		  "	 indices[pidx] +=1;\n" +
		  "  ((&itype&dtype1VectorStorage)resPartStorages[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), v2Values[i]));\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompOtherVsDummy = "{\n" +
		  "&itypeLow subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "&itypeLow[] v2Indices = v2.getIndices();\n" +
		  "for (int i = 0; i < v2Indices.length; i++) {\n" +
		  "  &itypeLow gidx = v2Indices[i];\n" +
		  "  int pidx = (int) (gidx / subDim);\n" +
		  "  &itypeLow subidx = gidx % subDim;\n" +
		  "  ((&itype&dtype1VectorStorage)resPartStorages[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), 1));\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompOtherVsSortedAll = "{\n" +
		  "&itypeLow subDim = (v1.getDim() + v1.getNumPartitions() - 1) / v1.getNumPartitions();\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "for (int i = 0; i < v1.getDim(); i++) {\n" +
		  "  int pidx = (int) (i / subDim);\n" +
		  "  &itypeLow subidx = i % subDim;\n" +
		  "	((&itype&dtype1VectorStorage)resPartStorages[pidx]).set(subidx, op.apply(parts[pidx].get(subidx), v2.get(i)));\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompBuildCompVector = "{\n" +
		  "&itype&dtype1Vector[] parts = v1.getPartitions();\n" +
		  "&itype&dtype1Vector[] res = new &itype&dtype1Vector[parts.length];\n" +
		  "for (int i = 0; i <parts.length; i++){\n" +
		  "  &itype&dtype1Vector part = parts[i];\n" +
		  "  res[i] = new &itype&dtype1Vector(part.getMatrixId(), part.getRowId(), part.getClock(), part.getDim(), (&itype&dtype1VectorStorage)resPartStorages[i]);\n" +
		  "}\n" +
		  "if (op.isInplace()){\n" +
		  "	v1.setPartitions(res);\n" +
		  "	return v1;\n" +
		  "} else {\n" +
		  "	return new Comp&itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), res, v1.getSubDim());\n" +
		  "}\n" +
		  "}\n\n";

  private static String CompDenseVsDummy = "{\n" +
		  "&dtype1Low[] partValues = part.getStorage().getValues();\n" +
		  "&dtype1Low[] resPartValues = resPartStorage.getValues();\n" +
		  "for (int j = 0; j < partValues.length; j++) {\n" +
		  "	if (v2.hasKey(j + base)) {\n" +
		  "		resPartValues[j] = op.apply(partValues[j], v2.get(j + base));\n" +
		  "	}\n" +
		  "}\n" +
		  "}\n\n";

  private static String otherVsDummy = "{\n" +
		  "&itypeLow[] v2Indices = v2.getIndices();\n" +
		  "int[] v2Values = v2.getValues();\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itype&dtype1VectorStorage storage = v1.getStorage();\n" +
		  "&itypeLow size = v2.size();\n" +
		  "for (int i = 0; i < size; i++) {\n" +
		  "	&itypeLow idx = v2Indices[i];\n" +
		  "	if (newStorage.isSorted()){\n" +
		  "		resValues[i] = op.apply(storage.get(idx), v2Values[i]);\n" +
		  "	} else {\n" +
		  "		newStorage.set(idx, op.apply(storage.get(idx), v2Values[i]));\n" +
		  "	}\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsDummy1 = "{\n" +
		  "int v1Pointor = 0;\n" +
		  "int v2Pointor = 0;\n" +
		  "&itypeLow size1 = v1.size();\n" +
		  "&itypeLow size2 = v2.size();\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&itypeLow[] v2Indices = v2.getIndices();\n" +
		  "&itypeLow[] resIndices = newStorage.getIndices();\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "int globalPointor = 0;\n" +
		  "while (v1Pointor < size1 && v2Pointor < size2) {\n" +
		  "  if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {\n" +
		  "		resIndices[globalPointor] = v1Indices[v1Pointor];\n" +
		  "		resValues[globalPointor] = op.apply(v1Values[v1Pointor], 1);\n" +
		  "		v1Pointor++;\n" +
		  "		v2Pointor++;\n" +
		  "		globalPointor++;\n" +
		  "	} else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "  		resIndices[globalPointor] = v1Indices[v1Pointor];\n" +
		  "  		resValues[globalPointor] = op.apply(v1Values[v1Pointor], 0);\n" +
		  "  		globalPointor++;\n" +
		  "		}\n" +
		  "		v1Pointor++;\n" +
		  "	} else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "  		resIndices[globalPointor] = v2Indices[v2Pointor];\n" +
		  "  		resValues[globalPointor] = op.apply(0, 1);\n" +
		  "  		globalPointor++;\n" +
		  "		}\n" +
		  "		v2Pointor++;\n" +
		  "	}\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";

  private static String sortedVsDummy2 = "{\n" +
		  "int v1Pointor = 0;\n" +
		  "int v2Pointor = 0;\n" +
		  "&itypeLow size1 = v1.size();\n" +
		  "&itypeLow size2 = v2.size();\n" +
		  "&dtype1Low[] resValues = newStorage.getValues();\n" +
		  "&itypeLow[] v1Indices = v1.getStorage().getIndices();\n" +
		  "&dtype1Low[] v1Values = v1.getStorage().getValues();\n" +
		  "&itypeLow[] v2Indices = v2.getIndices();\n" +
		  "while (v1Pointor < size1 && v2Pointor < size2) {\n" +
		  "	if (v1Indices[v1Pointor] == v2Indices[v2Pointor]) {\n" +
		  "		newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 1));\n" +
		  "		v1Pointor++;\n" +
		  "		v2Pointor++;\n" +
		  "	} else if (v1Indices[v1Pointor] < v2Indices[v2Pointor]) {\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "			newStorage.set(v1Indices[v1Pointor], op.apply(v1Values[v1Pointor], 0));\n" +
		  "		}\n" +
		  "		v1Pointor++;\n" +
		  "	} else { // v1Indices[v1Pointor] > v2Indices[v2Pointor]\n" +
		  "		if (op.getOpType()!= OpType.INTERSECTION){\n" +
		  "			newStorage.set(v2Indices[v2Pointor], op.apply(0, 1));\n" +
		  "		}\n" +
		  "		v2Pointor++;\n" +
		  "	}\n" +
		  "}\n" +
		  "return new &itype&dtype1Vector(v1.getMatrixId(), v1.getRowId(), v1.getClock(), v1.getDim(), newStorage);\n" +
		  "}\n\n";


  public static UtilsFuncs getInstance() {
	if (executor == null) {
	  synchronized (UtilsFuncs.class) {
		if (executor == null) {
		  try {
			String[] itypes = new String[]{"Int", "Long"};
			String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
			String[] names = new String[]{"rightZeroOperation", "leftZeroOperation", "replaceOperation", "otherVsDense",
					"otherVsSparse", "otherVsSorted", "denseVsOtherFaster", "otherVsDenseFaster", "denseVsDense",
					"sparseVsDense", "sortedVsDense", "sortedVsSparse1", "sortedVsSparse2", "sortedVsSorted1",
					"sortedVsSorted2", "allSparse", "sparseVsOther", "CompDenseVsOther", "CompLeftZeroOperation",
					"CompSparseVsOther", "CompOtherVsDense", "CompSortedVsOther1", "CompSortedVsOther", "CompTransStorage",
					"CompOtherVsSparse", "CompOtherVsSorted", "CompOtherVsDummy", "CompDenseVsDummy", "CompOtherVsSortedAll",
					"CompBuildCompVector", "otherVsDummy", "sortedVsDummy1", "sortedVsDummy2"};
			String template = "import com.tencent.angel.ml.math2.ufuncs.executor.*;\n" +
					"import com.tencent.angel.ml.math2.storage.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.Binary;\n" +
					"import com.tencent.angel.ml.math2.vector.*;\n" +
					"import com.tencent.angel.ml.math2.vector.Vector;\n" +
					"import it.unimi.dsi.fastutil.ints.*;\n" +
					"import it.unimi.dsi.fastutil.longs.*;\n" +
					"import java.util.*;\n" +
					"import com.tencent.angel.ml.math2.ufuncs.expression.OpType;\n" +
					"import it.unimi.dsi.fastutil.objects.ObjectIterator;\n\n";
			String declTemplate = "public &ReturnType &Name(&Parameters)";
			String bodyTemplate = "&Declaration &Body";
			String declare = "", body = "", method = "";
			for (int q = 0; q < names.length; q++) {
			  for (int i = 0; i < itypes.length; i++) {
				for (int j = 0; j < dtypes.length; j++) {
				  declare = declTemplate.replaceAll("&ReturnType", "Vector")
						  .replaceAll("&Name", names[q])
						  .replaceAll("&Parameters", "&itype&dtype1Vector v1, &itypeDummyVector v2, " +
								  "&itype&dtype1VectorStorage newStorage, Binary op");

				  if (names[q] == "otherVsDummy") {
					body = otherVsDummy;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
					continue;
				  } else if (names[q] == "sortedVsDummy1") {
					body = sortedVsDummy1;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
					continue;
				  } else if (names[q] == "sortedVsDummy2") {
					body = sortedVsDummy2;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
					continue;
				  } else if (names[q] == "allSparse") {
					body = allSparse;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "sparseVsOther") {
					body = sparseVsOther;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "denseVsOtherFaster") {
					body = denseVsOtherFaster;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "CompOtherVsDummy") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, " +
									"&itypeDummyVector v2, Storage[] resPartStorages, Binary op");

					body = CompOtherVsDummy;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "CompOtherVsSortedAll") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, " +
									"&itypeDummyVector v2, Storage[] resPartStorages, Binary op");

					body = CompOtherVsSortedAll;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "CompDenseVsDummy" && itypes[i] == "Int") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "&itype&dtype1Vector part, " +
									"&itypeDummyVector v2, &itype&dtype1VectorStorage resPartStorage, int base, Binary op");

					body = CompDenseVsDummy;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "CompSparseVsOther") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "&itype&dtype1Vector part, " +
									"&itypeDummyVector v2, &itype&dtype1VectorStorage resPartStorage, int base, Binary op");

					body = CompSparseVsOther;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "CompSortedVsOther") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "&itype&dtype1Vector part, " +
									"&itypeDummyVector v2, &itype&dtype1VectorStorage resPartStorage, int base, Binary op");

					body = CompSortedVsOther;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  } else if (names[q] == "CompSortedVsOther1") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "&itype&dtype1Vector part, " +
									"&itypeDummyVector v2, &itype&dtype1VectorStorage resPartStorage, int base, Binary op");

					body = CompSortedVsOther1;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					template += method;
				  }


				  if (names[q] == "rightZeroOperation") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "&itype&dtype1Vector v1, &itype&dtype1VectorStorage newStorage, Binary op");
					body = rightZeroOperation;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);

					template += method;
					continue;
				  } else if (names[q] == "replaceOperation") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "&itype&dtype1Vector v1, &itype&dtype1VectorStorage newStorage")
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					body = replaceOperation;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);

					template += method;
					continue;
				  } else if (names[q] == "CompTransStorage") {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "Comp&itype&dtype1Vector v1,Storage[] resPartStorages")
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					body = CompTransStorage;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);

					template += method;
					continue;
				  } else if (names[q] == "CompBuildCompVector") {
					declare = declTemplate.replaceAll("&ReturnType", "Vector")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, Storage[] resPartStorages, Binary op")
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);
					body = CompBuildCompVector;
					method = bodyTemplate.replaceAll("&Declaration", declare)
							.replaceAll("&Body", body)
							.replaceAll("&itypeLow", itypes[i].toLowerCase())
							.replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j]);

					template += method;
					continue;
				  }

				  for (int p = j; p < dtypes.length; p++) {
					declare = declTemplate.replaceAll("&ReturnType", "void")
							.replaceAll("&Name", names[q])
							.replaceAll("&Parameters", "Comp&itype&dtype1Vector v1, " +
									"&itype&dtype2Vector v2, Storage[] resPartStorages, Binary op")
							.replaceAll("&itype", itypes[i])
							.replaceAll("&dtype1", dtypes[j])
							.replaceAll("&dtype2", dtypes[p]);
					if (names[q] == "CompOtherVsSparse") {
					  body = CompOtherVsSparse;
					  method = bodyTemplate.replaceAll("&Declaration", declare)
							  .replaceAll("&Body", body)
							  .replaceAll("&itypeLow", itypes[i].toLowerCase())
							  .replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							  .replaceAll("&dtype2Low", dtypes[p].toLowerCase())
							  .replaceAll("&itype", itypes[i])
							  .replaceAll("&dtype1", dtypes[j])
							  .replaceAll("&dtype2", dtypes[p]);
					  template += method;
					  continue;
					} else if (names[q] == "CompOtherVsSorted") {
					  body = CompOtherVsSorted;
					  method = bodyTemplate.replaceAll("&Declaration", declare)
							  .replaceAll("&Body", body)
							  .replaceAll("&itypeLow", itypes[i].toLowerCase())
							  .replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							  .replaceAll("&dtype2Low", dtypes[p].toLowerCase())
							  .replaceAll("&itype", itypes[i])
							  .replaceAll("&dtype1", dtypes[j])
							  .replaceAll("&dtype2", dtypes[p]);
					  template += method;
					  continue;
					} else if (names[q] == "CompOtherVsSortedAll") {
					  body = CompOtherVsSortedAll;
					  method = bodyTemplate.replaceAll("&Declaration", declare)
							  .replaceAll("&Body", body)
							  .replaceAll("&itypeLow", itypes[i].toLowerCase())
							  .replaceAll("&dtype1Low", dtypes[j].toLowerCase())
							  .replaceAll("&dtype2Low", dtypes[p].toLowerCase())
							  .replaceAll("&itype", itypes[i])
							  .replaceAll("&dtype1", dtypes[j])
							  .replaceAll("&dtype2", dtypes[p]);
					  template += method;
					  continue;
					}

					if (names[q].startsWith("Comp")) {
					  declare = declTemplate.replaceAll("&ReturnType", "void")
							  .replaceAll("&Name", names[q])
							  .replaceAll("&Parameters", "&itype&dtype1Vector part, " +
									  "&itype&dtype2Vector v2, &itype&dtype1VectorStorage resPartStorage, int base, Binary op")
							  .replaceAll("&itype", itypes[i])
							  .replaceAll("&dtype1", dtypes[j])
							  .replaceAll("&dtype2", dtypes[p]);
					  if (names[q] == "CompDenseVsOther") {
						body = CompDenseVsOther;
					  } else if (names[q] == "CompLeftZeroOperation" && itypes[i] == "Int") {
						body = CompLeftZeroOperation;
					  } else if (names[q] == "CompSparseVsOther") {
						body = CompSparseVsOther;
					  } else if (names[q] == "CompOtherVsDense" && itypes[i] == "Int") {
						body = CompOtherVsDense;
					  } else if (names[q] == "CompSortedVsOther1") {
						body = CompSortedVsOther1;
					  } else if (names[q] == "CompSortedVsOther") {
						body = CompSortedVsOther;
					  } else {
						body = "";
						declare = "";
						continue;
					  }
					} else if (names[q] == "leftZeroOperation") {
					  declare = declTemplate.replaceAll("&ReturnType", "void")
							  .replaceAll("&Name", names[q])
							  .replaceAll("&Parameters", "&itype&dtype2Vector v2, &itype&dtype1VectorStorage newStorage, Binary op")
							  .replaceAll("&itype", itypes[i])
							  .replaceAll("&dtype1", dtypes[j])
							  .replaceAll("&dtype2", dtypes[p]);
					  body = leftZeroOperation;
					} else {
					  declare = declTemplate.replaceAll("&ReturnType", "Vector")
							  .replaceAll("&Name", names[q])
							  .replaceAll("&Parameters", "&itype&dtype1Vector v1, " +
									  "&itype&dtype2Vector v2, &itype&dtype1VectorStorage newStorage, Binary op")
							  .replaceAll("&itype", itypes[i])
							  .replaceAll("&dtype1", dtypes[j])
							  .replaceAll("&dtype2", dtypes[p]);

					  if (names[q] == "denseVsDense" && itypes[i] == "Int") {
						body = denseVsDense;
					  } else if (names[q] == "sparseVsDense" && itypes[i] == "Int") {
						body = sparseVsDense;
					  } else if (names[q] == "sortedVsDense" && itypes[i] == "Int") {
						body = sortedVsDense;
					  } else if (names[q] == "otherVsDense") {
						body = otherVsDense;
					  } else if (names[q] == "otherVsSparse") {
						body = otherVsSparse;
					  } else if (names[q] == "otherVsSorted") {
						body = otherVsSorted;
					  } else if (names[q] == "denseVsOtherFaster") {
						body = denseVsOtherFaster;
					  } else if (names[q] == "otherVsDenseFaster") {
						body = otherVsDenseFaster;
					  } else if (names[q] == "sortedVsSparse1") {
						body = sortedVsSparse1;
					  } else if (names[q] == "sortedVsSparse2") {
						body = sortedVsSparse2;
					  } else if (names[q] == "sortedVsSorted1") {
						body = sortedVsSorted1;
					  } else if (names[q] == "sortedVsSorted2") {
						body = sortedVsSorted2;
					  } else if (names[q] == "allSparse") {
						body = allSparse;
					  } else if (names[q] == "sparseVsOther") {
						body = sparseVsOther;
					  } else {
						body = "";
						continue;
					  }
					}

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
			}

			ClassBodyEvaluator ee = new ClassBodyEvaluator();
			executor = (UtilsFuncs) ee.createFastClassBodyEvaluator(
					new Scanner(null, new StringReader(
							template)),
					UtilsFuncs.class,
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
