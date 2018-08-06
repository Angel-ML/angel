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

package com.tencent.angel.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tencent.angel.ml.metric.Metric;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class KryoUtils {
  private static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>();
  private static ThreadLocal<KryoReflectionFactorySupport> kryoRefs = new ThreadLocal<KryoReflectionFactorySupport>();

  private static Kryo getKryo(){
    Kryo kryo = kryos.get();
    if(kryo == null) {
      kryo = new Kryo();
      kryos.set(kryo);
    }

    return kryo;
  }

  private static KryoReflectionFactorySupport getKryoRef(){
    KryoReflectionFactorySupport kryoRef = kryoRefs.get();
    if(kryoRef == null) {
      kryoRef = new KryoReflectionFactorySupport();
      kryoRefs.set(kryoRef);
    }

    return kryoRef;
  }

  public static byte[] serializeAlgoMetric(Metric value) {
    Kryo kryo = getKryo();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    Output output = new Output(baos);
    kryo.writeClassAndObject(output, value);
    output.flush();
    output.close();
    return baos.toByteArray();
  }

  public static Metric deserializeAlgoMetric(byte [] data) {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    Input input = new Input(bais);
    KryoReflectionFactorySupport kryoRef = getKryoRef();
    return ((Metric)kryoRef.readClassAndObject(input));
  }
}
