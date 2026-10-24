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
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Registration;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tencent.angel.ml.metric.Metric;
import de.javakaffee.kryoserializers.KryoReflectionFactorySupport;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;

public class KryoUtils {
  private static ThreadLocal<Kryo> kryos = new ThreadLocal<Kryo>();
  private static ThreadLocal<KryoReflectionFactorySupport> kryoRefs =
    new ThreadLocal<KryoReflectionFactorySupport>();

  private static Kryo getKryo() {
    Kryo kryo = kryos.get();
    if (kryo == null) {
      kryo = new Kryo();
      kryos.set(kryo);
    }

    return kryo;
  }

  private static KryoReflectionFactorySupport getKryoRef() {
    KryoReflectionFactorySupport kryoRef = kryoRefs.get();
    if (kryoRef == null) {
      kryoRef = new KryoReflectionFactorySupport();
      // The class and the object of a metric are read separately, so the graph state must be kept
      // between them; deserializeAlgoMetric resets it once the whole metric has been read
      kryoRef.setAutoReset(false);
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

  /**
   * Deserialize an algorithm metric. The class header is checked before any object is created, so
   * data that does not describe a {@link Metric} is rejected without instantiating the class it
   * names.
   *
   * @param data serialized metric, written by {@link #serializeAlgoMetric(Metric)}
   * @return the metric, or null if a null metric was serialized
   * @throws KryoException if the data does not describe a {@link Metric}
   */
  public static Metric deserializeAlgoMetric(byte[] data) {
    ByteArrayInputStream bais = new ByteArrayInputStream(data);
    Input input = new Input(bais);
    KryoReflectionFactorySupport kryoRef = getKryoRef();
    try {
      Registration registration = kryoRef.readClass(input);
      if (registration == null) {
        return null;
      }

      Class<?> type = registration.getType();
      if (!Metric.class.isAssignableFrom(type)) {
        throw new KryoException(
            "Serialized algorithm metric has type " + type.getName() + ", which is not a "
                + Metric.class.getName());
      }
      return (Metric) kryoRef.readObject(input, type);
    } finally {
      kryoRef.reset();
    }
  }
}
