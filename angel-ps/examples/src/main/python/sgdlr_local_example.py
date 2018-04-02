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
from pyangel.ml.classification.runner import LRRunner

class SGDLRLocalExample(object):

    def __init__(self):
        self.conf = Configuration()

    def set_conf(self):

        # Feature number of train data
        feature_num = 124
        # Total iteration number
        epoch_num = 20
        # Validation sample Ratio
        v_ratio = 0.1
        # Data format, libsvm or dummy
        data_fmt = "libsvm"
        # Train batch number per epoch.
        sp_ratio = 1.0
        # Batch number
        batch_num = 10

        # Learning rate
        learn_rate = 1.0
        # Decay of learning rate
        decay = 0.1
        # Regularization coefficient
        reg = 0.2

        # Set local deploy mode
        self.conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")

        # Set basic configuration keys
        self.conf.set_boolean("mapred.mapper.new-api", True)
        self.conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat')
        self.conf.set_boolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, True)

        # Set data format
        self.conf.set(MLConf.ML_DATAFORMAT, data_fmt)

        # set angel resource parameters #worker, #task, #PS
        self.conf.set_int(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_PS_NUMBER, 1)

        # set sgd LR algorithm parameters #feature #epoch
        self.conf.set(MLConf.ML_FEATURE_NUM, str(feature_num))
        self.conf.set(MLConf.ML_EPOCH_NUM, str(epoch_num))
        self.conf.set(MLConf.ML_BATCH_SAMPLE_Ratio, str(sp_ratio))
        self.conf.set(MLConf.ML_VALIDATE_RATIO, str(v_ratio))
        self.conf.set(MLConf.ML_LEARN_RATE, str(learn_rate))
        self.conf.set(MLConf.ML_LEARN_DECAY, str(decay))
        self.conf.set(MLConf.ML_REG_L2, str(reg))

    def train_on_local_cluster(self):
        self.set_conf()
        input_path = "../data/exampledata/LRLocalExampleData/a9a.train"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        save_path = LOCAL_FS + TMP_PATH + "/model"
        log_path = LOCAL_FS + TMP_PATH + "/log"

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        # Set save model path
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, save_path)
        # Set log path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, log_path)
        # Set actionType train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN)


        runner = LRRunner()
        runner.train(self.conf)


    def inc_train(self):
        self.set_conf()
        input_path = "../data/exampledata/LRLocalExampleData/a9a.train"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        load_path = LOCAL_FS + TMP_PATH + "/model"
        save_path = LOCAL_FS + TMP_PATH + "/newmodel"
        log_path = LOCAL_FS + TMP_PATH + "/log"

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        # Set load model path
        self.conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, load_path)
        # Set save model path
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, save_path)
        # Set log path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, log_path)
        # Set actionType incremental train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_INC_TRAIN)

        runner = LRRunner()
        runner.inc_train(self.conf)


    def predict(self):
        self.set_conf()
        input_path = "../data/exampledata/LRLocalExampleData/a9a.test"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = System.getProperty("java.io.tmpdir", "/tmp")
        load_path = LOCAL_FS + TMP_PATH + "/model"
        save_path = LOCAL_FS + TMP_PATH + "/model"
        log_path = LOCAL_FS + TMP_PATH + "/log"
        predict_path = LOCAL_FS + TMP_PATH + "/predict"

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        # Set load model path
        self.conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, load_path)
        # Set predict result path
        self.conf.set(AngelConf.ANGEL_PREDICT_PATH, predict_path)
        # Set log path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, log_path)
        # Set actionType prediction
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT())

        runner = LRRunner()

        runner.predict(self.conf)

example = SGDLRLocalExample()
example.train_on_local_cluster()
