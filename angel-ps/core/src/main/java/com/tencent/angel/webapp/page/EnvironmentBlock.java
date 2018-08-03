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

package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.tencent.angel.conf.AngelConf;
import com.tencent.angel.master.app.AMContext;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.util.*;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

// import org.apache.hadoop.conf.Configuration;
// import java.lang.reflect.Field;

public class EnvironmentBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  EnvironmentBlock(AMContext amctx) {
    amContext = amctx;
  }


  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel Environment"));
    html.h1("Runtime Information");

    TABLE<Hamlet> run_info_table = html.table();
    String UsrHome = System.getProperty("user.home");
    String UsrDir = System.getProperty("user.dir");
    String UsrName = System.getProperty("user.name");
    String JavaHome = System.getProperty("java.home");
    String OsNmae = System.getProperty("os.name");
    String JavaVersion = System.getProperty("java.version");
    run_info_table.tr().th(_TH, "NAME").th(_TH, "VALUE")._();
    run_info_table.tr().td("UsrHome").td(UsrHome)._().tr().td("UsrDir").td(UsrDir)._().tr()
        .td("UsrName").td(UsrName)._().tr().td("JavaHome").td(JavaHome)._().tr().td("OsNmae")
        .td(OsNmae)._().tr().td("JavaVersion").td(JavaVersion)._();
    run_info_table._();
    html.h1("    ");

    html.h1("Angel Properties");
    TABLE<Hamlet> angel_properties_table = html.table();
    angel_properties_table.tr().th(_TH, "NAME").th(_TH, "VALUE")._();
    AngelConf angelConfiguration = new AngelConf(this.amContext.getConf());
    Properties propertiesConfiguration = angelConfiguration.getAngelProps();
    SortedMap sortedMap = new TreeMap(propertiesConfiguration);
    Set<Object> set = sortedMap.keySet();
    Iterator<Object> propertiesSortedKeys = set.iterator();
    Object key;
    while (propertiesSortedKeys.hasNext()) {
      key = propertiesSortedKeys.next();
      angel_properties_table.tr().td(String.valueOf(key))
          .td((String) propertiesConfiguration.get(key))._();
    }
    angel_properties_table._();
    html.h1("    ");


    TBODY<TABLE<Hamlet>> tbody =
        html.h1("System Properties").table("#jobs").thead().tr().th(_TH, "NAME").th(_TH, "VALUE")
            ._()._().tbody();
    Properties properties = System.getProperties();
    String propertiesName;
    String propertiesValue;
    for (Iterator<?> names = (Iterator<?>) properties.propertyNames(); names.hasNext();) {
      propertiesName = (String) names.next();
      propertiesValue = properties.getProperty(propertiesName);
      tbody.tr().td(propertiesName).td(propertiesValue).td().span().$title(propertiesName)._()._()
          .td().span().$title(propertiesValue)._()._()._();
    }
    tbody._()._();

  }


}
