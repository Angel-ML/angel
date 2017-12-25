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

class AngelClientFactory(object):
    """
    Angel client factory, it support two types client now: LOCAL and YARN
    """
    @staticmethod
    def get(map, conf):
        """
        get an instance of AngelClient
        :param conf: a Java HashMap which contains the entry set
                     to build a Hadoop Configuration instance
        :return: AngelClient(AngelLocalClient or AngelYarnClient)
        """
        return conf._jvm.com.tencent.angel.client.AngelClientFactory.get(map, conf._jconf)
