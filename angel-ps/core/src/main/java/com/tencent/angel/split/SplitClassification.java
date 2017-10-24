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

package com.tencent.angel.split;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SplitClassification {

  private String[] locations;
  private boolean useNewAPI;
  private List<org.apache.hadoop.mapred.InputSplit> splitsOldAPI = null;
  private List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI = null;

  // private Configuration conf;
  
  public SplitClassification(){
    
  }

  public SplitClassification(List<org.apache.hadoop.mapred.InputSplit> splitsOldAPI,
      List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI, String[] locations,
      boolean useNewAPI) {
    this.locations = locations;
    this.splitsOldAPI = splitsOldAPI;
    this.splitsNewAPI = splitsNewAPI;
    this.useNewAPI = useNewAPI;
  }

  public SplitClassification(List<org.apache.hadoop.mapred.InputSplit> splitsOldAPI,
      List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI, boolean useNewAPI) {
    this.splitsOldAPI = splitsOldAPI;
    this.splitsNewAPI = splitsNewAPI;
    this.useNewAPI = useNewAPI;
  }

  public String[] getLocations() {
    return locations;
  }

  public void setLocations(String[] locations) {
    this.locations = locations;
  }

  public boolean isUseNewAPI() {
    return useNewAPI;
  }

  public void setUseNewAPI(boolean useNewAPI) {
    this.useNewAPI = useNewAPI;
  }

  public List<org.apache.hadoop.mapred.InputSplit> getSplitsOldAPI() {
    return splitsOldAPI;
  }

  public void setSplitsOldAPI(List<org.apache.hadoop.mapred.InputSplit> splitsOldAPI) {
    this.splitsOldAPI = splitsOldAPI;
  }

  public List<org.apache.hadoop.mapreduce.InputSplit> getSplitsNewAPI() {
    return splitsNewAPI;
  }

  public void setSplitsNewAPI(List<org.apache.hadoop.mapreduce.InputSplit> splitsNewAPI) {
    this.splitsNewAPI = splitsNewAPI;
  }

  public void addSplit(org.apache.hadoop.mapreduce.InputSplit split) {
    splitsNewAPI.add(split);
  }

  public void addSplit(org.apache.hadoop.mapred.InputSplit split) {
    splitsOldAPI.add(split);
  }

  public int getSplitNum() {
    if (useNewAPI) {
      return splitsNewAPI.size();
    } else {
      return splitsOldAPI.size();
    }
  }

  public org.apache.hadoop.mapreduce.InputSplit getSplitNewAPI(int splitIndex) {
    return splitsNewAPI.get(splitIndex);
  }

  public org.apache.hadoop.mapred.InputSplit getSplitOldAPI(int splitIndex) {
    return splitsOldAPI.get(splitIndex);
  }

  @Override
  public String toString() {
    return "SplitClassification [locations=" + Arrays.toString(locations) + ", useNewAPI="
        + useNewAPI + ", splitsOldAPI=" + splitsOldAPI + ", splitsNewAPI=" + splitsNewAPI + "]";
  }
  
  @SuppressWarnings({"rawtypes", "unchecked"})
  public void serialize(DataOutputStream output) throws IOException {
    output.writeBoolean(useNewAPI);
    if(useNewAPI) {
      int size = splitsNewAPI.size();
      output.writeInt(size);
      output.writeInt(locations.length);
      for(int i = 0; i < locations.length; i++) {
        output.writeUTF(locations[i]);
      }
      if(size > 0) {
        output.writeUTF(splitsNewAPI.get(0).getClass().getName());
        SerializationFactory factory = new SerializationFactory(new Configuration());
        Serializer serializer = factory.getSerializer(splitsNewAPI.get(0).getClass());
        serializer.open(output);       
        for(int i = 0; i < size; i++){          
          serializer.serialize(splitsNewAPI.get(i));
        }
      }
    } else {
      int size = splitsOldAPI.size();
      output.writeInt(size);
      output.writeInt(locations.length);
      for(int i = 0; i < size; i++) {
        output.writeUTF(locations[i]);
      }
      if(size > 0) {
        output.writeUTF(splitsOldAPI.get(0).getClass().getName());      
        SerializationFactory factory = new SerializationFactory(new Configuration());
        Serializer serializer = factory.getSerializer(splitsOldAPI.get(0).getClass());
        serializer.open(output);       
        for(int i = 0; i < size; i++){          
          serializer.serialize(splitsOldAPI.get(i));
        }
      }
    }
  }
  
  @SuppressWarnings("unchecked")
  public void deserialize(DataInputStream input) throws IOException, ClassNotFoundException {
    useNewAPI = input.readBoolean();
    int size = input.readInt();
    int locSize = input.readInt();
    locations = new String[locSize];
    for(int i = 0; i < locSize; i++) {
      locations[i] = input.readUTF();
    }
    
    if(useNewAPI) {
      if(size > 0) {
        String splitClass = input.readUTF();
        splitsNewAPI = new ArrayList<org.apache.hadoop.mapreduce.InputSplit>(size);
        SerializationFactory factory = new SerializationFactory(new Configuration());
        Deserializer<? extends org.apache.hadoop.mapreduce.InputSplit> deSerializer =
            factory.getDeserializer((Class<? extends org.apache.hadoop.mapreduce.InputSplit>) Class
                .forName(splitClass));

        deSerializer.open(input);
        for(int i = 0; i < size; i++) {
          splitsNewAPI.add(deSerializer.deserialize(null));
        }
      }

    } else {
      if(size > 0) {
        String splitClass = input.readUTF();
        splitsOldAPI = new ArrayList<org.apache.hadoop.mapred.InputSplit>(size);
        SerializationFactory factory = new SerializationFactory(new Configuration());
        Deserializer<? extends org.apache.hadoop.mapred.InputSplit> deSerializer =
            factory.getDeserializer((Class<? extends org.apache.hadoop.mapred.InputSplit>) Class
                .forName(splitClass));

        deSerializer.open(input);
        for(int i = 0; i < size; i++) {
          splitsOldAPI.add(deSerializer.deserialize(null));
        }
      }
    }
  }
}
