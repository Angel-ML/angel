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

from pyangel.conf import AngelConf
from pyangel.conf import Configuration
from pyangel.ml.conf import MLConf
from pyangel.ml.client.angel_client_factory import AngelClientFactory
from pyangel.ml_runner import MLRunner

class FMRunner(object):
    """
    Training job to obtain a FM model
    """

    def train(self, conf):
        client = AngelClientFactory.get(conf)

        model = conf._jvm.com.tencent.angel.ml.factorizationmachines.FMModel(conf._jconf, None)

        client.startPSServer()
        client.loadModel(model)
        client.runTask('com.tencent.angel.ml.factorizationmachines.FMTrainTask')
        client.waitForCompletion()
        client.saveModel(model)

        client.stop()
