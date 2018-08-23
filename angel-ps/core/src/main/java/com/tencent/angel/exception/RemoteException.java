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


package com.tencent.angel.exception;

import org.xml.sax.Attributes;

import java.io.IOException;
import java.lang.reflect.Constructor;

public class RemoteException extends IOException {
  private static final long serialVersionUID = 1L;
  private String className;

  public RemoteException(String className, String msg) {
    super(msg);
    this.className = className;
  }

  public String getClassName() {
    return this.className;
  }

  public IOException unwrapRemoteException(Class<?>[] lookupTypes) {
    if (lookupTypes == null)
      return this;
    for (Class lookupClass : lookupTypes) {
      if (!lookupClass.getName().equals(getClassName()))
        continue;
      try {
        return instantiateException(lookupClass.asSubclass(IOException.class));
      } catch (Exception e) {
        return this;
      }
    }

    return this;
  }

  public IOException unwrapRemoteException() {
    try {
      Class realClass = Class.forName(getClassName());
      return instantiateException(realClass.asSubclass(IOException.class));
    } catch (Exception e) {
    }
    return this;
  }

  private IOException instantiateException(Class<? extends IOException> cls) throws Exception {
    Constructor cn = cls.getConstructor(new Class[] {String.class});
    cn.setAccessible(true);
    String firstLine = getMessage();
    int eol = firstLine.indexOf(10);
    if (eol >= 0) {
      firstLine = firstLine.substring(0, eol);
    }
    IOException ex = (IOException) cn.newInstance(new Object[] {firstLine});
    ex.initCause(this);
    return ex;
  }

  public static RemoteException valueOf(Attributes attrs) {
    return new RemoteException(attrs.getValue("class"), attrs.getValue("message"));
  }
}