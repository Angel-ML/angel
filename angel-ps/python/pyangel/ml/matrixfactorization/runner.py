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

import tempfile

from hadoop.local_fs import LocalFileSystem

from pyangel.conf import AngelConf
from pyangel.conf import Configuration
from pyangel.ml.conf import MLConf
from pyangel.ml.client.angel_client_factory import AngelClientFactory
from pyangel.ml_runner import MLRunner

class MatrixFactorizationRunner(MLRunner):

    def train(self, conf):
        """
        Training job to obtain a model
        :param conf: configuration for parameter settings
        """
        conf.set(AngelConf.ANGEL_TASK_USER_TASKCLASS, 'com.tencent.angel.ml.matrixfactorization.MFTrainTask')

        # Create an angel job client
        client = AngelClientFactory.get(conf)

        # Submit this application
        client.startPSServer()

        # Create a MFModel
        mfModel = conf._jvm.com.tencent.angel.ml.matrixfactorization.MFModel(conf._jconf, None)

        # Load model meta to client
        client.loadModel(mfModel)

        # Start
        client.runTask('com.tencent.angel.ml.matrixfactorization.MFTrainTask')

        # Run user task and wait for completion
        # User task is set in AngelConf.ANGEL_TASK_USER_TASKCLASS
        client.waitForCompletion()

        # Save the trained model to HDFS
        client.saveModel(mfModel)

        # Stop
        client.stop()
