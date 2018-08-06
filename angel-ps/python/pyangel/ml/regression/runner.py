#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017-2018 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https://opensource.org/licenses/Apache-2.0
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

from pyangel.conf import AngelConf
from pyangel.ml_runner import MLRunner
from pyangel.ml.client.angel_client_factory import AngelClientFactory

class LinearRegRunner(MLRunner):
    """
    Run linear regression task on angel
    """

    def train(self, conf):
        """
        Run linear regression train task
        :param conf:  configuration of a algorithm and resource
        :return:
        """

        conf['angel.worker.matrix.transfer.request.timeout.ms'] = 60000

        jconf = conf.dict_to_jconf()
        super(LinearRegRunner, self).train(conf, conf._jvm.com.tencent.angel.ml.regression.linear.LinearRegModel(jconf, None), 'com.tencent.angel.ml.regression.linear.LinearRegTrainTask')

    def predict(self, conf):
        """
        Run linear regression predict task
        :param conf: configuration of algorithm and resource
        :return:
        """
        conf['angel.worker.matrix.transfer.request.timeout.ms'] = 60000

        jconf = conf.dict_to_jconf()
        super(LinearRegRunner, self).train(conf, conf._jvm.com.tencent.angel.ml.regression.linear.LinearRegModel(jconf, None), 'com.tencent.angel.ml.regression.linear.LinearRegPredictTask')


    def inc_train(self, conf):
        """
        Run incremental train task
        :param conf: configuration of aalgorithm and resource
        :return:
        """
        conf['angel.worker.matrix.transfer.request.timeout.ms'] = 60000
        conf[AngelConf.ANGEL_TASK_USER_TASKCLASS] = 'com.tencent.angel.ml.regression.linear.LinearRegTrainTask'

        # Create an angel job client
        client = AngelClientFactory.get(conf)

        # Submit this application
        client.startPSServer()

        # Create a model
        model = conf._jvm.com.tencent.angel.ml.regression.linear.LinearRegModel(conf._jconf)

        # Load model meta to client
        client.loadModel(model)

        # Run user task
        client.runTask('com.tencent.angel.ml.regression.linear.LinearRegTrainTask')

        # Wait for completion,
        # User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
        client.waitForCompletion()

        # Save the incremental trained model to HDFS
        client.saveModel(model)

        # Stop
        client.stop()
