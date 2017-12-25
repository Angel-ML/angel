#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https:#opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing, software distributed under the License is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

__all__ = ['MLConf']


from pyangel.context import Configuration


class MLParams(type):
    """
    Angel machine learning parameters.
    """

    def __getattr__(cls, item):
        _jvm = Configuration._jvm
        value = _jvm.com.tencent.angel.ml.utils.ReflectionUtils.getAttr(item)
        return str(value)

class MLConf(metaclass=MLParams):
    pass
