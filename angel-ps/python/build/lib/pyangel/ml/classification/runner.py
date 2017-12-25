#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License") you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https:#opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

from pyangel.ml_runner import MLRunner

class LRRunner(MLRunner):
    """
    Run logistic regression task on angel
    """

    def train(self, conf):
        """
        Run LR train task
        :param conf: configuration of algorithm and resource
        """
        conf.set_int("angel.worker.matrixtransfer.request.timeout.ms", 60000)

        super(LRRunner, self).train(conf, conf._jvm.com.tencent.angel.ml.classification.lr.LRModel(conf._jconf, None), 'com.tencent.angel.ml.classification.lr.LRTrainTask')


    def predict(self, conf):
        """
        Run LR predict task
        :param conf: configuration of algorithm and resource
        """
        conf.set_int("angel.worker.matrix.transfer.request.timeout.ms", 60000)
        predict(conf, conf._jvm.com.tencent.angel.ml.classification.lr.LRModel(conf._jconf, None), 'com.tencent.angel.ml.classification.lr.LRPredictTask')

    def incTrain(self, conf):
        """
        Run LR incremental train task
        :param conf: configuration of algorithm and resource
        """
        conf.set_int("angel.worker.matrix.transfer.request.timeout.ms", 60000)
        train(conf, conf._jvm.com.com.tencent.angel.ml.classification.lr.LRModel(conf._jconf, None), 'com.tencent.angel.ml.classification.lr.LRTrainTask')
