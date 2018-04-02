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
from pyangel.context import Configuration
from pyangel.ml.conf import MLConf
from pyangel.ml.matrixfactorization.runner import MatrixFactorizationRunner

class MFLocalExample(object):

    def __init__(self):
        self.conf= Configuration()
        self.MLConf = MLConf()

    def set_conf(self):
        inputPath = "../../data/exampledata/MFLocalExampleData"
        # Set local deploy mode
        self.conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")
        # Set basic self.configuration keys
        self.conf.set_boolean("mapred.mapper.new-api", True)
        self.conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat')
        self.conf.set_boolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, True)

        # set angel resource parameters #worker, #task, #PS
        self.conf.set_int(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_PS_NUMBER, 1)

        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()

        # Set trainning data, and save model path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model")
        self.conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/log")
        # Set actionType train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN)

        # Set MF algorithm parameters
        self.conf.set(MLConf.ML_MF_RANK, "200")
        self.conf.set(MLConf.ML_EPOCH_NUM, "8")
        self.conf.set(MLConf.ML_MF_ROW_BATCH_NUM, "2")
        self.conf.set(MLConf.ML_MF_ITEM_NUM, "1683")
        self.conf.set(MLConf.ML_MF_LAMBDA, "0.01")
        self.conf.set(MLConf.ML_MF_ETA, "0.0054")


    def train(self):
        self.set_conf()
        runner = MatrixFactorizationRunner()
        runner.train(self.conf)

example = MFLocalExample()
example.train()
