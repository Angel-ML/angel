package com.tencent.angel.utils;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.tencent.angel.ml.metrics.Metric;
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
