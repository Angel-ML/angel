#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited] = a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License") you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https:#opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing] = software distributed under the License is
# distributed on an "AS IS" BASIS] = WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#


from pyangel.conf import AngelConf
from pyangel.context import Configuration
from pyangel.ml.conf import MLConf
from pyangel.ml.gbdt.runner import GBDTRunner

class GBDTExample(object):

    def __init__(self):
        self.conf= Configuration()
        self.MLConf = MLConf()

    def set_conf(self):
        # Input Path, please modify ${YOUR_ANGEL_HOME} as your local angel installation path,
        # e.g. if your path is /home/angel/angel_1.3.0, your input_path should be:
        # "file:///home/angel/angel_1.3.0/data/exampledata/GBDTLocalExampleData/agaricus.txt.train",
        # and your out_path could be: "file:///home/angel/angel_1.3.0/data/output"
        # if you need, you can delete the annotation mark before Line35,Line36,Line61,Line62, so
        # there is no need for you to pass the configs every time you submit the pyangel job.
        #input_path = "file:///${YOUR_ANGEL_HOME}/data/exampledata/GBDTLocalExampleData/agaricus.txt.train"
        #output_path = "file:///${YOUR_ANGEL_HOME}/data/output"
        # Feature number of train data
        feature_num = 127
        # Number of nonzero features
        feature_nzz = 25
        # Tree number
        tree_num = 2
        # Tree depth
        tree_depth = 2
        # Split number
        split_num = 10
        # Feature sample ratio
        sample_ratio = 1.0

        # Data format
        data_fmt = "libsvm"

        # Learning rate
        learn_rate = 0.01

        # Use local deploy mode and dummy data spliter
        self.conf[AngelConf.ANGEL_DEPLOY_MODE] = "LOCAL"

        # set input] = output path
        self.conf[AngelConf.ANGEL_INPUTFORMAT_CLASS] = 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat'
        #self.conf[AngelConf.ANGEL_TRAIN_DATA_PATH] = input_path
        #self.conf[AngelConf.ANGEL_SAVE_MODEL_PATH] = output_path

        # Set GBDT algorithm parameters
        self.conf[MLConf.ML_FEATURE_NUM] = str(feature_num)
        self.conf[MLConf.ML_FEATURE_NNZ] = str(feature_nzz)
        self.conf[MLConf.ML_GBDT_TREE_NUM] = str(tree_num)
        self.conf[MLConf.ML_GBDT_TREE_DEPTH] = str(tree_depth)
        self.conf[MLConf.ML_GBDT_SPLIT_NUM] = str(split_num)
        self.conf[MLConf.ML_GBDT_SAMPLE_RATIO] = str(sample_ratio)
        self.conf[MLConf.ML_LEARN_RATE] = str(learn_rate)

    def train(self):
        self.set_conf()

        runner = GBDTRunner()
        runner.train(self.conf)



    def predict(self):
        self.set_conf()
        # Load Model from HDFS.
        TMP_PATH = tempfile.gettempdir()
        self.conf["gbdt.split.feature"] = TMP_PATH + "/out/xxx"
        self.conf["gbdt.split.value"] = TMP_PATH + "/out/xxx"

        runner = GBDTRunner()

        runner.predict(conf)


example = GBDTExample()
example.train()
