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


package com.tencent.angel.ml.math2;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import org.apache.velocity.Template;
import org.apache.velocity.VelocityContext;
import org.apache.velocity.app.VelocityEngine;
import org.apache.velocity.runtime.RuntimeConstants;
import org.apache.velocity.runtime.resource.loader.ClasspathResourceLoader;
import org.junit.BeforeClass;
import org.junit.Test;

public class VelocityUtils {

  private static VelocityEngine ve;
  private static String[] itypes = new String[]{"Int", "Long"};
  private static String[] dtypes = new String[]{"Double", "Float", "Long", "Int"};
  private static String basePath = "F:\\git.code.oa.com\\angel\\angel-ps\\core\\src\\main\\java";

  @BeforeClass
  public static void init() {
    /*  first, get and initialize an engine  */
    ve = new VelocityEngine();
    ve.setProperty(RuntimeConstants.RESOURCE_LOADER, "classpath");
    ve.setProperty("classpath.resource.loader.class", ClasspathResourceLoader.class.getName());
    ve.init();
  }

  private String merge(VelocityContext context, Template template) {
    /* now render the template into a StringWriter */
    StringWriter writer = new StringWriter();
    template.merge(context, writer);
    /* show the World */
    return writer.toString();
  }

  private void write(String fname, String content) {
    File file = new File(fname);
    if (file.exists()) {
      file.delete();
    }
    try {
      FileOutputStream fos = new FileOutputStream(file);
      fos.write(content.getBytes());
      fos.close();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private String getPath(String packageName) {
    return String.format("%s\\%s\\", basePath, packageName.replace(".", "\\"));
  }

  @Test
  public void denseStorages() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/storage/densestorage.vm");
    String path = getPath("com.tencent.angel.ml.math2.storage");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    String itype = "Int";
    for (String dtype : dtypes) {
      context.put("itype", itype);
      context.put("dtype", dtype);
      String content = merge(context, template);
      // System.out.println(content);
      write(path + String.format("%s%sDenseVectorStorage.java", itype, dtype), content);
    }
  }

  @Test
  public void sparseStorages() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/storage/sparsestorage.vm");
    String path = getPath("com.tencent.angel.ml.math2.storage");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    for (String itype : itypes) {
      for (String dtype : dtypes) {
        context.put("itype", itype);
        context.put("dtype", dtype);
        String content = merge(context, template);
        // System.out.println(content);
        write(path + String.format("%s%sSparseVectorStorage.java", itype, dtype), content);
      }
    }
  }

  @Test
  public void sortedStorages() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/storage/sortedstorage.vm");
    String path = getPath("com.tencent.angel.ml.math2.storage");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    for (String itype : itypes) {
      for (String dtype : dtypes) {
        context.put("itype", itype);
        context.put("dtype", dtype);
        String content = merge(context, template);
        // System.out.println(content);
        write(path + String.format("%s%sSortedVectorStorage.java", itype, dtype), content);
      }
    }
  }

  @Test
  public void compVector() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/vector/compvector.vm");
    String path = getPath("com.tencent.angel.ml.math2.vector");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    for (String itype : itypes) {
      for (String dtype : dtypes) {
        context.put("itype", itype);
        context.put("dtype", dtype);
        String content = merge(context, template);
        // System.out.println(content);
        write(path + String.format("Comp%s%sVector.java", itype, dtype), content);
      }
    }
  }

  @Test
  public void simpleVector() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/vector/simplevector.vm");
    String path = getPath("com.tencent.angel.ml.math2.vector");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    for (String itype : itypes) {
      for (String dtype : dtypes) {
        context.put("itype", itype);
        context.put("dtype", dtype);
        String content = merge(context, template);
        // System.out.println(content);
        write(path + String.format("%s%sVector.java", itype, dtype), content);
      }
    }
  }

  @Test
  public void rbMatrix() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/matrix/rbmatrix.vm");
    String path = getPath("com.tencent.angel.ml.math2.matrix");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    String[] comps = new String[]{"Comp", ""};
    for (String comp : comps) {
      context.put("comp", comp);
      for (String itype : itypes) {
        context.put("itype", itype);
        for (String dtype : dtypes) {
          context.put("dtype", dtype);
          String content = merge(context, template);
          // System.out.println(content);
          write(path + String.format("RB%s%s%sMatrix.java", comp, itype, dtype), content);
        }
      }
    }
  }

  @Test
  public void blasMatrix() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/matrix/blasmatrix.vm");
    String path = getPath("com.tencent.angel.ml.math2.matrix");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    String[] dtypes0 = new String[]{"Double", "Float"};
    for (String dtype : dtypes0) {
      context.put("dtype", dtype);
      context.put("dtypes", dtypes);
      String content = merge(context, template);
      // System.out.println(content);
      write(path + String.format("Blas%sMatrix.java", dtype), content);
    }
  }

  @Test
  public void simpleUnaryExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/simple/simpleunaryexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.simple");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "SimpleUnaryExecutor.java", content);
  }

  @Test
  public void simpleDotExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/simple/simpledotexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.simple");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "SimpleDotExecutor.java", content);
  }

  @Test
  public void simpleBinaryOutZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/simple/simplebinaryoutzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.simple");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    //  System.out.println(content);
    write(path + "SimpleBinaryOutZAExecutor.java", content);
  }

  @Test
  public void simpleBinaryOutNonZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/simple/simplebinaryoutnonzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.simple");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "SimpleBinaryOutNonZAExecutor.java", content);
  }

  @Test
  public void simpleBinaryInZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/simple/simplebinaryinzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.simple");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "SimpleBinaryInZAExecutor.java", content);
  }

  @Test
  public void simpleBinaryInNonZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/simple/simplebinaryinnonzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.simple");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "SimpleBinaryInNonZAExecutor.java", content);
  }

  @Test
  public void binaryMatrixExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/matrix/binarymatrixexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.matrix");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes1", new String[]{"Double", "Float"});
    context.put("dtypes2", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "BinaryMatrixExecutor.java", content);
  }

  @Test
  public void dotMatrixExectutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/matrix/dotmatrixexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.matrix");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes1", new String[]{"Double", "Float"});
    context.put("dtypes2", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "DotMatrixExecutor.java", content);
  }

  @Test
  public void compBinaryExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/comp/compbinaryexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.comp");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "CompBinaryExecutor.java", content);
  }

  @Test
  public void compDotExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/comp/compdotexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.comp");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "CompDotExecutor.java", content);
  }

  @Test
  public void compUnaryExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/comp/compunaryexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.comp");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "CompUnaryExecutor.java", content);
  }

  @Test
  public void compReduceExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/comp/compreduceexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.comp");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "CompReduceExecutor.java", content);
  }

  @Test
  public void mixedBinaryInNonZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/mixed/mixedbinaryinnonzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedBinaryInNonZAExecutor.java", content);
  }

  @Test
  public void mixedBinaryOutNonZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/mixed/mixedbinaryoutnonzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedBinaryOutNonZAExecutor.java", content);
  }

  @Test
  public void mixedDotExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/mixed/mixeddotexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedDotExecutor.java", content);
  }

  @Test
  public void mixedBinaryInZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/mixed/mixedbinaryinzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedBinaryInZAExecutor.java", content);
  }

  @Test
  public void mixedBinaryOutZAExecutor() {
    /*  next, get the Template  */
    Template template = ve.getTemplate("vmfiles/executor/mixed/mixedbinaryoutzaexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedBinaryOutZAExecutor.java", content);
  }

  @Test
  public void mixedBinaryInALLExecutor() {
    /*  next, get the Template  */

    Template template = ve.getTemplate("vmfiles/executor/mixed/mixedbinaryinallexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedBinaryInAllExecutor.java", content);
  }

  @Test
  public void mixedBinaryOutALLExecutor() {
    /*  next, get the Template  */

    Template template = ve.getTemplate("vmfiles/executor/mixed/mixedbinaryoutallexecutor.vm");
    String path = getPath("com.tencent.angel.ml.math2.ufuncs.executor.mixed");

    /*  create a context and add data */
    VelocityContext context = new VelocityContext();
    context.put("itypes", itypes);
    context.put("dtypes", dtypes);
    String content = merge(context, template);
    // System.out.println(content);
    write(path + "MixedBinaryOutAllExecutor.java", content);
  }
}
