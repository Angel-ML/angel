#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License") you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https#opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

from pyangel.conf import AngelConf
from pyangel.ml.conf import MLConf
from pyangel.ml_runner import MLRunner

class GBDTRunner(MLRunner):
    featureNum = 0
    featureNonzero = 0
    maxTreeNum = 0
    maxTreeDepth = 0
    splitNum = 0
    featureSampleRatio = 0.0

    def train(self, conf):
        conf["angel.worker.matrixtransfer.request.timeout.ms"] = 60000
        featNum = int(conf[MLConf.ML_FEATURE_NUM])
        psNumber = int(conf[AngelConf.ANGEL_PS_NUMBER])
        if (featNum % psNumber != 0):
            featNum = (featNum / psNumber + 1) * psNumber
            conf.set_int(MLConf.ML_FEATURE_NUM, featNum)

        conf[MLConf.ML_FEATURE_NUM] = featNum
        jconf = conf.dict_to_jconf()
        super(GBDTRunner, self).train(conf, conf._jvm.com.tencent.angel.ml.GBDT.GBDTModel(jconf, None), 'com.tencent.angel.ml.GBDT.GBDTTrainTask')

    def predict(self, conf):
        conf["angel.worker.matrix.transfer.request.timeout.ms"] = 60000
        super(MLRunner, self).predict(conf, conf._jvm.com.tencent.angel.ml.GBDT.GBDTModel(conf._jconf, None), 'com.tencent.angel.ml.GBDT.GBDTPredictTask')

    def incTrain(self, conf):
        """
        Incremental training job to obtain a model based on a trained model
        """
        conf.set_int("angel.worker.matrix.transfer.request.timeout.ms", 60000)
        train(conf)
