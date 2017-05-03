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

package com.tencent.angel.ml.feature;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NamedNodeMap;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

public class FeatureInfoParser {
  private static final Log LOG = LogFactory.getLog(FeatureInfoParser.class);

  public static FeatureInfo parseFeatureInfoFromXml() throws ParserConfigurationException,
      SAXException, IOException {
    FeatureInfo featureInfo = new FeatureInfo();
    InputStream inputStream = new FileInputStream(new File("feature_conf.xml"));

    // Thread.currentThread().getContextClassLoader().getResourceAsStream("feature_conf.xml");
    DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
    DocumentBuilder builder = factory.newDocumentBuilder();
    Document document = builder.parse(inputStream);
    Element element = document.getDocumentElement();
    NodeList fields = element.getElementsByTagName("match");
    NodeList matches = fields.item(0).getChildNodes();

    int size = matches.getLength();

    for (int i = 0; i < size; i++) {
      if ("field".equals(matches.item(i).getNodeName())) {
        NamedNodeMap attrs = matches.item(i).getAttributes();
        featureInfo.addMatch(attrs.item(1).getNodeValue(),
            Integer.valueOf(attrs.item(0).getNodeValue()));
        LOG.debug(attrs.item(0).getNodeValue() + "," + attrs.item(1).getNodeValue());
      }
    }

    NodeList genFeatures = element.getElementsByTagName("gen_feature").item(0).getChildNodes();

    int featureTypeNum = genFeatures.getLength();
    for (int i = 0; i < featureTypeNum; i++) {
      if (genFeatures.item(i).getAttributes() == null) {
        continue;
      }
      if ("id_features".equals(genFeatures.item(i).getAttributes().item(0).getNodeValue())) {
        NodeList nl = genFeatures.item(i).getChildNodes();
        int sizes = nl.getLength();
        for (int j = 0; j < sizes; j++) {
          if ("feature".equals(nl.item(j).getNodeName())) {
            NamedNodeMap attrs = nl.item(j).getAttributes();
            featureInfo.addSimpleFeature(attrs.item(0).getNodeValue());
            LOG.debug(attrs.item(0).getNodeValue());
          }
        }
      } else if ("comb_features".equals(genFeatures.item(i).getAttributes().item(0).getNodeValue())) {
        NodeList nl = genFeatures.item(i).getChildNodes();
        int sizes = nl.getLength();
        for (int j = 0; j < sizes; j++) {
          if ("feature".equals(nl.item(j).getNodeName())) {
            NamedNodeMap attrs = nl.item(j).getAttributes();
            featureInfo.addCombineFeatures(attrs.item(1).getNodeValue(), attrs.item(0)
                .getNodeValue().split(";"));
            LOG.debug(attrs.item(1).getNodeValue() + "," + attrs.item(0).getNodeValue());
          }
        }
      }
    }

    return featureInfo;
  }
}
