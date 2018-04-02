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
from pyangel.ml.clustering.k_means import KMeansRunner

class KmeansLocalExample(object):

    def __init__(self):
        self.conf = Configuration()
        self.MLConf = MLConf()

    def set_conf(self):
        data_fmt = "libsvm"

        # Cluster center number
        center_num = 3
        # Feature number of train data
        feature_num = 4
        # Total iteration number
        epoch_num = 20
        # Sample ratio per mini-batch
        sample_ratio = 1.0
        # C
        c = 0.15

        # Set local deploy mode
        self.conf.set(AngelConf.ANGEL_DEPLOY_MODE, "LOCAL")

        # Set basic self.configuration key
        self.conf.set_boolean("mapred.mapper.new-api", True)
        self.conf.set(AngelConf.ANGEL_INPUTFORMAT_CLASS, 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat')
        self.conf.set_boolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, True)

        # Set angel resource parameters #worker, #task, #PS
        self.conf.set_int(AngelConf.ANGEL_WORKERGROUP_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_WORKER_TASK_NUMBER, 1)
        self.conf.set_int(AngelConf.ANGEL_PS_NUMBER, 1)

        # Set Kmeans algorithm parameters #cluster #feature #epoch
        self.conf.set(MLConf.KMEANS_CENTER_NUM, str(center_num))
        self.conf.set(MLConf.ML_FEATURE_NUM, str(feature_num))
        self.conf.set(MLConf.ML_EPOCH_NUM, str(epoch_num))
        self.conf.set(MLConf.KMEANS_SAMPLE_RATIO_PERBATCH, str(sample_ratio))
        self.conf.set(MLConf.kMEANS_C, str(c))

        # Set data format
        self.conf.set(MLConf.ML_DATAFORMAT, data_fmt)

    def train(self):
        self.set_conf()
        input_path = "data/exampledata/clusteringLocalExampleData/iris"
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_TRAIN_DATA_PATH, input_path)
        # Set save model path
        self.conf.set(AngelConf.ANGEL_SAVE_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model")
        # Set log sava path
        self.conf.set(AngelConf.ANGEL_LOG_PATH, LOCAL_FS + TMP_PATH + "/kmeansLog/log")
        # Set actionType train
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_TRAIN)

        runner = KMeansRunner()
        runner.train(self.conf)

        angel_client = AngelClientFactory.get(self.conf)
        angel_client.stop()

    def predict_onLocal_cluster(self):
        self.set_conf()
        LOCAL_FS = LocalFileSystem.DEFAULT_FS
        TMP_PATH = tempfile.gettempdir()
        # Set trainning data path
        self.conf.set(AngelConf.ANGEL_PREDICT_DATA_PATH, input_path)
        # Set load model path
        self.conf.set(AngelConf.ANGEL_LOAD_MODEL_PATH, LOCAL_FS + TMP_PATH + "/model")
        # Set predict result path
        self.conf.set(AngelConf.ANGEL_PREDICT_PATH, LOCAL_FS + TMP_PATH + "/predict")
        # Set actionType prediction
        self.conf.set(AngelConf.ANGEL_ACTION_TYPE, MLConf.ANGEL_ML_PREDICT)

        runner = KMeansRunner()
        runner.predict(self.conf)

        angel_client = AngelClientFactory.get(self.conf)
        angel_client.stop()

example = KmeansLocalExample()
example.train()
