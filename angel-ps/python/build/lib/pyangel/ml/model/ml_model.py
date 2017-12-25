#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https://opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

from pyangel.context import Configuration

class LinearRegModel(object):
    LR_WEIGHT_MAT = "lr_weight"

    _jvm = None

    def __init__(self, conf, _ctx = None):
        _jvm = conf._jvm
        feaNum = conf.get_int(MLConf.ML_FEATURE_NUM, 10000)
        weight = _jvm.PSModel[TDoubleVector](LR_WEIGHT_MAT, 1, feaNum).setAverage(true)


