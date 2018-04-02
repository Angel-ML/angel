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
from pyangel.ml.regression.runner import LinearRegRunner

class LinearRegLocalExample(object):
    """
    Linear Regression Example used for user test, similar to "com.tencent.angel.example.ml.LinearRegLocalExample".
    """

    def __init__(self):
        self.conf= Configuration()

    def set_conf(self):
        """
        Set up self.configuration for runtime environment.
        """
        # Feature number of train data
        feature_num = 101
        # Total iteration number
        epoch_num = 20
        # Validation sample ratio
        v_ratio = 0.5
        # Data format,libsvm or dummy
        data_fmt = 'libsvm'
        # Train batch number per epoch
        sp_ratio = 1

        # Learning rate
        learn_rate = 0.1
        # Decay of learning rate
        decay = 0.01
        # Regularization coefficient
        reg = 0

        # Set job queue, if you use YARN deploy mode, you can set job queue by
        # self.conf.set('mapreduce.job.queue.name', 'default')

        # Set local deploy mode
        self.conf.set(AngelConf.ANGEL_DEPLOY_MODE, 'LOCAL')

        # Set basic self.configuration keys
        self.conf.set_boolean('mapred.mapper.new-api', True)
        self.conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat')
        self.conf.set_boolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, True)

        # Set data format
        self.conf.set(MLConf.ML_DATAFORMAT, data_fmt)

        # set angel resource parameters #worker, #tast, #ps
        self.conf.set_int(AngelConf.ANGEL_WORKERGROUP_NUMBER, 2)
        self.conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 10)
        self.conf.set_int(AngelConf.ANGEL_PS_NUMBER, 2)

        # set sgd LR algorithim parameters # feature # epoch
        self.conf.set(MLConf.ML_FEATURE_NUM, str(feature_num))
        self.conf.set(MLConf.ML_EPOCH_NUM, str(epoch_num))
        self.conf.set(MLConf.ML_BATCH_SAMPLE_Ratio, str(sp_ratio))
        self.conf.set(MLConf.ML_VALIDATE_RATIO, str(v_ratio))
        self.conf.set(MLConf.ML_LEARN_RATE, str(learn_rate))
        self.conf.set(MLConf.ML_LEARN_DECAY, str(decay))
        self.conf.set(MLConf.ML_REG_L2, str(reg))


    def train_on_local_cluster(self):
        """
        Train model on local cluster
        """
        self.set_conf()
        input_path = '../data/exampledata/LinearRegression'
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        log_path = ".src/test/log"
        model_path = 'file:///tmp/angel/model'


        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, model_path)
        self.conf.set(AngelConf.ANGEL_LOG_PATH, log_path)
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN)
        self.conf.set("fs.defaultFS", LOCAL_FS + TMP_PATH)

        runner = LinearRegRunner()
        runner.train(self.conf)


    def inc_train(self):
        self.set_conf()
        input_path = "../data/exampledata/LinearRegression/LinearReg100.train"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        log_path = "./src/test/log"

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
        # Set load model path
        self.conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model")
        # Set save model path
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/newmodel")
        # Set actionType incremental train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_INC_TRAIN())
        # Set log path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, logPath)

        runner = LinearRegRunner()
        runner.incTrain(self.conf)

    def predict(self):
        self.set_conf()
        input_path = "../data/exampledata/LinearRegression/LinearReg100.train"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()

        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, inputPath)
        # Set load model path
        self.conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model")
        # Set predict result path
        self.conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict")
        # Set actionType prediction
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT())
        runner = LinearRegRunner()

        runner.predict(self.conf)


example = LinearRegLocalExample()
example.train_on_local_cluster()
