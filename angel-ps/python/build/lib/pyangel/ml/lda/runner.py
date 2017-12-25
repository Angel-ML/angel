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

class LDARunner(MLRunner):


    # Training job to obtain a model
    override
    def train(self, conf):
        conf.set_int(AngelConf.ANGEL_WORKER_MAX_ATTEMPTS, 1)
        conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
        conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'BalanceInputFormat')

        client = AngelClientFactory.get(conf)

        client.startPSServer()
        client.loadModel(new LDAModel(conf))
        client.runTask([LDATrainTask])
        client.waitForCompletion()

        client.stop()
        jconf = conf.dict_to_jconf()
        super(MatrixFactorizationRunner, self).train(conf, conf._jvm.com.tencent.angel.ml.matrixfactorization.MFModel(jconf, None), 'com.tencent.angel.ml.matrixfactorization.MFTrainTask')

    def predict(self, conf):
        """
        Using a model to predict with unobserved samples
        """
        conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
        conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'BalanceInputFormat')
        client = AngelClientFactory.get(conf)

        client.startPSServer()
        client.loadModel(LDAModel(conf))
        client.runTask('LDAInferTask')
        client.waitForCompletion()

        client.stop()
