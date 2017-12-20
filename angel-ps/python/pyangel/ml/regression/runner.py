#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https:#opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

from pyangel.conf import AngelConf
from pyangel.context import Configuration
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

        conf.set_int("angel.worker.matrix.transfer.request.timeout.ms", 60000)
        conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, 'com.tencent.angel.ml.regression.linear.LinearRegTrainTask')

        # Create an angel job client
        client = AngelClientFactory.get(conf)

        # Submit this application
        client.startPSServer()

        # Create a linear reg model
        model = conf._jvm.com.tencent.angel.ml.regression.linear.LinearRegModel(conf._jconf, None)

        # Load model meta to client
        client.loadModel(model)

        # Run user task
        client.runTask("com.tencent.angel.ml.regression.linear.LinearRegTrainTask")

        # Wait for completion,
        # User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
        client.waitForCompletion()

        # Save the trained model to HDFS
        client.saveModel(model)

        # Stop
        client.stop()

    def predict(self, conf):
        """
        Run linear regression predict task
        :param conf: configuration of algorithm and resource
        :return:
        """
        conf.set_int("angel.worker.matrix.transfer.request.timeout.ms", 60000)
        conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, 'com.tencent.angel.ml.regression.linear.LinearRegPredictTask')

        # Create an angel job client
        client = AngelClientFactory.get(conf)

        # Submit this application
        client.startPSServer()

        # Create a model
        model = conf._jvm.com.tencent.angel.ml.regression.linear.LinearRegModel(conf._jconf)

        # Add the model meta to client
        client.loadModel(model)

        # Run user task
        client.runTask('com.tencent.angel.ml.regression.linear.LinearRegPredictTask')

        # Wait for completion,
        # User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
        client.waitForCompletion()

        # Stop
        client.stop()

    def inc_train(self, conf):
        """
        Run incremental train task
        :param conf: configuration of aalgorithm and resource
        :return:
        """
        conf.set_int("angel.worker.matrix.transfer.request.timeout.ms", 60000)
        conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, 'com.tencent.angel.ml.regression.linear.LinearRegTrainTask')

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
