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
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.utils;

import com.tencent.angel.split.SplitClassification;
import com.tencent.angel.split.SplitInfo;
import com.tencent.angel.protobuf.generated.WorkerMasterServiceProtos.SplitInfoProto;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Serialize/Deserialize tool for training data split.
 */
public class SerdeUtils {
  private static SerializationFactory factory;

  public static SplitClassification deSerilizeSplitProtos(List<SplitInfoProto> splitInfoList,
      Configuration conf) throws ClassNotFoundException, IOException {
    boolean isUseNewAPI = conf.getBoolean("mapred.mapper.new-api", false);
    if (isUseNewAPI) {
      List<org.apache.hadoop.mapreduce.InputSplit> splitList =
          new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
      for (SplitInfoProto splitInfo : splitInfoList) {
        splitList.add(deSerilizeNewSplit(splitInfo.getSplitClass(), splitInfo.getSplit()
            .toByteArray(), conf));
      }

      SplitClassification splits = new SplitClassification(null, splitList, true);
      return splits;
    } else {
      List<org.apache.hadoop.mapred.InputSplit> splitList =
          new ArrayList<org.apache.hadoop.mapred.InputSplit>();
      for (SplitInfoProto splitInfo : splitInfoList) {
        splitList.add(deSerilizeOldSplit(splitInfo.getSplitClass(), splitInfo.getSplit()
            .toByteArray(), conf));
      }

      SplitClassification splits = new SplitClassification(splitList, null, true);
      return splits;
    }
  }


  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SplitInfo serilizeSplit(org.apache.hadoop.mapreduce.InputSplit split,
      Configuration conf) throws IOException {
    if (factory == null) {
      factory = new SerializationFactory(conf);
    }
    DataOutputBuffer out = new DataOutputBuffer(1024);

    try {
      Serializer serializer = factory.getSerializer(split.getClass());
      serializer.open(out);
      serializer.serialize(split);
      SplitInfo ret = new SplitInfo(split.getClass().getName(), out.getData());
      return ret;
    } finally {
      out.close();
    }
  }

  @SuppressWarnings({"unchecked", "rawtypes"})
  public static SplitInfo serilizeSplit(org.apache.hadoop.mapred.InputSplit split,
      Configuration conf) throws IOException {
    if (factory == null) {
      factory = new SerializationFactory(conf);
    }
    DataOutputBuffer out = new DataOutputBuffer(1024);

    try {
      Serializer serializer = factory.getSerializer(split.getClass());
      serializer.open(out);
      serializer.serialize(split);
      SplitInfo ret = new SplitInfo(split.getClass().getName(), out.getData());
      return ret;
    } finally {
      out.close();
    }
  }

  public static List<SplitInfo> serilizeSplits(SplitClassification splits, Configuration conf)
      throws IOException {

    List<SplitInfo> splitInfoList = new ArrayList<SplitInfo>();
    if (splits.isUseNewAPI()) {
      List<org.apache.hadoop.mapreduce.InputSplit> splitList = splits.getSplitsNewAPI();
      for (org.apache.hadoop.mapreduce.InputSplit split : splitList) {
        splitInfoList.add(serilizeSplit(split, conf));
      }
    } else {
      List<org.apache.hadoop.mapred.InputSplit> splitList = splits.getSplitsOldAPI();
      for (org.apache.hadoop.mapred.InputSplit split : splitList) {
        splitInfoList.add(serilizeSplit(split, conf));
      }
    }

    return splitInfoList;
  }

  @SuppressWarnings("unchecked")
  public static org.apache.hadoop.mapreduce.InputSplit deSerilizeNewSplit(SplitInfo splitInfo,
      Configuration conf) throws IOException, ClassNotFoundException {
    if (factory == null) {
      factory = new SerializationFactory(conf);
    }

    ByteArrayInputStream in = null;

    try {
      Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deSerializer =
          factory.getDeserializer((Class<? extends org.apache.hadoop.mapreduce.InputSplit>) Class
              .forName(splitInfo.getSplitClass()));
      in = new ByteArrayInputStream(splitInfo.getSplit());
      deSerializer.open(in);
      return deSerializer.deserialize(null);
    } finally {
      if (in != null) {
        in.close();
      }
    }

  }

  @SuppressWarnings("unchecked")
  public static org.apache.hadoop.mapred.InputSplit deSerilizeOldSplit(SplitInfo splitInfo,
      Configuration conf) throws ClassNotFoundException, IOException {
    if (factory == null) {
      factory = new SerializationFactory(conf);
    }

    ByteArrayInputStream in = null;

    try {
      Deserializer<? extends org.apache.hadoop.mapred.InputSplit> deSerializer =
          factory.getDeserializer((Class<? extends org.apache.hadoop.mapred.InputSplit>) Class
              .forName(splitInfo.getSplitClass()));
      in = new ByteArrayInputStream(splitInfo.getSplit());
      deSerializer.open(in);
      return deSerializer.deserialize(null);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }

  public static SplitClassification deSerilizeSplits(List<SplitInfo> splitInfoList,
      Configuration conf) throws ClassNotFoundException, IOException {
    boolean isUseNewAPI = conf.getBoolean("mapred.mapper.new-api", false);
    if (isUseNewAPI) {
      List<org.apache.hadoop.mapreduce.InputSplit> splitList =
          new ArrayList<org.apache.hadoop.mapreduce.InputSplit>();
      for (SplitInfo splitInfo : splitInfoList) {
        splitList.add(deSerilizeNewSplit(splitInfo, conf));
      }

      SplitClassification splits = new SplitClassification(null, splitList, true);
      return splits;
    } else {
      List<org.apache.hadoop.mapred.InputSplit> splitList =
          new ArrayList<org.apache.hadoop.mapred.InputSplit>();
      for (SplitInfo splitInfo : splitInfoList) {
        splitList.add(deSerilizeOldSplit(splitInfo, conf));
      }

      SplitClassification splits = new SplitClassification(splitList, null, true);
      return splits;
    }
  }

  @SuppressWarnings("unchecked")
  public static org.apache.hadoop.mapreduce.InputSplit deSerilizeNewSplit(String className,
      byte[] data, Configuration conf) throws IOException, ClassNotFoundException {
    if (factory == null) {
      factory = new SerializationFactory(conf);
    }

    ByteArrayInputStream in = null;

    try {
      Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deSerializer =
          factory.getDeserializer((Class<? extends org.apache.hadoop.mapreduce.InputSplit>) Class
              .forName(className));
      in = new ByteArrayInputStream(data);
      deSerializer.open(in);
      return deSerializer.deserialize(null);
    } finally {
      if (in != null) {
        in.close();
      }
    }

  }

  @SuppressWarnings("unchecked")
  public static org.apache.hadoop.mapred.InputSplit deSerilizeOldSplit(String className,
      byte[] data, Configuration conf) throws ClassNotFoundException, IOException {
    if (factory == null) {
      factory = new SerializationFactory(conf);
    }

    ByteArrayInputStream in = null;

    try {
      Deserializer<? extends org.apache.hadoop.mapred.InputSplit> deSerializer =
          factory.getDeserializer((Class<? extends org.apache.hadoop.mapred.InputSplit>) Class
              .forName(className));
      in = new ByteArrayInputStream(data);
      deSerializer.open(in);
      return deSerializer.deserialize(null);
    } finally {
      if (in != null) {
        in.close();
      }
    }
  }
}
