/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 *
 */

package com.tencent.angel.ml.sketchML;

import com.tencent.angel.ml.feature.LabeledData;
import com.tencent.angel.ml.utils.DataParser;

import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class SketchMLTest {

  public List<LabeledData> loadData(String dataPath, int maxDim) throws IOException {
    List<LabeledData> ret = new ArrayList<LabeledData>();
    File dir = new File(dataPath);
    if (!dir.isDirectory()) {
      System.out.println(dataPath + " is no a directory!");
      return ret;
    }
    String[] files = dir.list();
    for (String fileName : files) {
      if (fileName.contains(".pkl"))
        continue;
      fileName = dataPath + "\\" + fileName;
      File curFile = new File(fileName);
      if (curFile.isFile() && curFile.exists()) {
        BufferedReader reader =
          new BufferedReader(new InputStreamReader(new FileInputStream(curFile)));
        String line = null;
        while (null != (line = reader.readLine())) {
          LabeledData ins = DataParser.parseVector(line, maxDim, "libsvm", true);
          ret.add(ins);
        }
      }
      System.out.println(fileName);
    }
    return ret;
  }

  public static void main(String[] argv) throws IOException {
    SketchMLTest test = new SketchMLTest();
    test
      .loadData("E:\\file\\liubo-gen-data\\data_file\\N700000_d47000_c2_nnz200_sp0.200000", 47000);
  }
}
