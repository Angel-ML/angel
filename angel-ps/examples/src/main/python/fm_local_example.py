#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License") you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

import tempfile

from hadoop.local_fs import LocalFileSystem

from pyangel.conf import AngelConf
from pyangel.context import Configuration
from pyangel.ml.conf import MLConf
from pyangel.ml.client.angel_client_factory import AngelClientFactory
from pyangel.ml.factorizationmachines.runner import FMRunner

class FMLocalExample(oject):

    def __init__(self):
        self.conf = Configuration()

    def set_conf(self):
        """
        set parameter values of self.conf
        """

        # Feature number of train data
        feature_num = 236
        # Total iteration number
        epoch_num = 20
        # Rank
        rank = 5
        # Regularization parameters
        reg0 = 0.0
        reg1 = 0.0
        reg2 = 0.001
        # Learn rage
        lr = 0.001
        stev = 0.1

        # Set local deploy mode
        self.conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")

        # Set basic self.configuration keys
        self.conf.set_boolean("mapred.mapper.new-api", True)
        self.conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat')
        self.conf.set_boolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, True)

        #set angel resource parameters #worker, #task, #PS
        self.conf.set_int(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_PS_NUMBER, 1)

        #set FM algorithm parameters #feature #epoch
        self.conf.set(MLConf.ML_FEATURE_NUM, str(feature_num))
        self.conf.set(MLConf.ML_EPOCH_NUM, str(epoch_num))
        self.conf.set(MLConf.ML_FM_RANK, str(rank))
        self.conf.set(MLConf.ML_LEARN_RATE, str(lr))
        self.conf.set(MLConf.ML_FM_REG0, str(reg0))
        self.conf.set(MLConf.ML_FM_REG1, str(reg1))
        self.conf.set(MLConf.ML_FM_REG2, str(reg2))
        self.conf.set(MLConf.ML_FM_V_STDDEV, str(stev))

    def train_on_local_cluster(self):
        self.set_conf()
        input_path = "./src/test/data/fm/food_fm_libsvm"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        save_path = LOCAL_FS + TMP_PATH + "/model"
        log_path = LOCAL_FS + TMP_PATH + "/LRlog"

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        # Set save model path
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, save_path)
        # Set log path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, log_path)
        # Set actionType train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN())

        runner = FMRunner()
        runner.train(self.conf)

        angel_client = AngelClientFactory.get(self.conf)
        angel_client.stop()

    def fm_classification(self):
        input_path = "./src/test/data/fm/a9a.train"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        save_path = LOCAL_FS + TMP_PATH + "/model"
        log_path = LOCAL_FS + TMP_PATH + "/LRlog"

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        # Set save model path
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, save_path)
        # Set log path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, log_path)
        # Set actionType train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN)
        # Set learnType
        self.conf.set(MLConf.ML_FM_LEARN_TYPE, "c")
        # Set feature number
        self.conf.set(MLConf.ML_FEATURE_NUM, str(124))

        runner = FMRunner()
        runner.train(self.conf)

        angel_client = AngelClientFactory.get(self.conf)
        angel_client.stop()

example = FMLocalExample()
example.train_on_local_cluster()
