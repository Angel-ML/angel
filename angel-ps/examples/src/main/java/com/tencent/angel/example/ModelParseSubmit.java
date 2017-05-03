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

package com.tencent.angel.example;

import com.tencent.angel.AppSubmitter;
import com.tencent.angel.client.AngelClient;
import com.tencent.angel.client.AngelClientFactory;
import com.tencent.angel.conf.AngelConfiguration;
import com.tencent.angel.conf.MatrixConfiguration;
import com.tencent.angel.ml.matrix.MatrixContext;
import com.tencent.angel.protobuf.generated.MLProtos;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

public class ModelParseSubmit implements AppSubmitter {
    private static final Log LOG = LogFactory.getLog(ModelParseSubmit.class);

    @Override
    public void submit(Configuration conf) throws Exception {
        conf.set(AngelConfiguration.ANGEL_TASK_USER_TASKCLASS, ModelParseTask.class.getName());
        AngelClient jobClient = AngelClientFactory.get(conf);
        int psNumber = conf.getInt(AngelConfiguration.ANGEL_PS_NUMBER,
                AngelConfiguration.DEFAULT_ANGEL_PS_NUMBER);
        MatrixContext mMatrix = new MatrixContext();
        mMatrix.setName("test");
        mMatrix.setRowNum(1);
        mMatrix.setColNum(100);
        mMatrix.setMaxRowNumInBlock(1);
        mMatrix.setMaxColNumInBlock(100 / psNumber);
        mMatrix.setRowType(MLProtos.RowType.T_DOUBLE_DENSE);
        mMatrix.set(MatrixConfiguration.MATRIX_AVERAGE, "true");
        jobClient.addMatrix(mMatrix);
        try {
            jobClient.start();
            jobClient.waitForCompletion();
            jobClient.stop();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

}
