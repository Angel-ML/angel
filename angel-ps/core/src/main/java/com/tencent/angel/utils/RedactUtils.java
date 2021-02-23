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

import com.tencent.angel.conf.AngelConf;
import org.apache.hadoop.conf.Configuration;

import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Reference from https://issues.apache.org/jira/browse/SPARK-18535
 * Redact the sensitive values in the given properties. If a map key matches the redaction pattern then
 * its value is replaced with a dummy text.
 */
public final class RedactUtils {
    private static final String REDACTION_REPLACEMENT_TEXT = "*********(redacted)";

    /**
     * Looks up the redaction regex from within the key value pairs and uses it to redact the rest
     * of the key value pairs. No care is taken to make sure the redaction property itself is not
     * redacted. So theoretically, the property itself could be configured to redact its own value
     * when printing.
     */
    public static Properties redactProperties(Configuration conf, Properties originalProperties) {
        Pattern redactionPattern = Pattern.compile(conf.get(AngelConf.ANGEL_REDACTION_REGEX, AngelConf.DEFAULT_ANGEL_REDACTION_REGEX));
        Properties redactedProperties = new Properties();

        originalProperties.forEach((key, value) -> {
            if(redactionPattern.matcher((String)key).find() || redactionPattern.matcher((String)value).find()) {
                redactedProperties.put(key, REDACTION_REPLACEMENT_TEXT);
            }else {
                redactedProperties.put(key, value);
            }
        });

        return redactedProperties;
    }
}
