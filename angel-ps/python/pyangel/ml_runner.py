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

import os
from abc import ABCMeta, abstractmethod

from pyangel.conf import AngelConf
from pyangel.ml.client.angel_client_factory import AngelClientFactory

class MLRunner(object):
    """
    Training job to obtain a model
    """

    def train(self, conf, model, class_name):
        """
        Default train method with standard training process. Don't Override this method
        :param conf: Configuration for angel application
        :param model: Data training model
        :param train_class: train task class name
        :return:
        """
        # Get Java HashMap instance which converted from a python dict
        jmap = conf.dict_to_jmap()
        client = AngelClientFactory.get(jmap, conf)

        client.startPSServer()
        client.loadModel(model)
        client.runTask(class_name)
        client.waitForCompletion()
        client.saveModel(model)

        client.stop()

    @abstractmethod
    def inc_train(self, conf):
        """
        Incremental training job to obtain a model based on a trained model
        :param conf:
        :return:
        """
        pass

    def predict(self, dconf, model, class_name):
        """
        Default predict method with standard predcit process. Don't try to override this Method
        :param conf:
        :return:
        """
        jmap = Configuration._dict_to_jmap(dconf)
        client = AngelClientFactory.get(jmap)
        client.startPSServer()
        client.loadModel(model)
        client.runTask(class_name)
        client.waitForCompletion()

        client.stop()

    def confirm_path(self, file_path, delete):
        """
        Judge the file path is right or doesn't exists
        :param file_path:
        :return:
        """
        if os.path.exists(file_path):
            if delete:
                rmr = raw_input("{} is already exists, do you want to remove it?[y/n]: ".format(file_path))
                if rmr == "y":
                    os.remove(file_path)
        else:
            print("Directory {} will be created".format(file_path))
