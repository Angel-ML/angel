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

import json
from collections import Counter
from threading import RLock

from pyangel.java_gateway import launch_gateway


__all__ = ['Configuration']

class Configuration(dict):
    """
    Configuration for Angel application.
    """

    _gateway = None
    _jvm = None
    _jconf = None
    _lock = RLock()
    _python_includes = None # zip and egg files that need to be added to PYTHONPATH

    PACKAGE_EXTENSIONS = ('.zip', '.egg', '.jar')

    def __init__(self, conf=None, gateway=None):
        Configuration._init(self, gateway=gateway, conf=conf)

    @classmethod
    def _init(cls, instance=None, gateway=None, conf=None):
        """
        Checks whether a Angel Configuration is initialized or not.
        Throws error if a Angel Configuration is already running.
        """
        with Configuration._lock:
            if not Configuration._gateway:
                Configuration._gateway = gateway or launch_gateway(conf)
                Configuration._jvm = Configuration._gateway.jvm
                Configuration._jconf = Configuration._jvm.com.tencent.angel.api.python.PythonGatewayServer.getJconf()


    def load(self, dict_config):
        """
         Use a Json string to pass the params to Configuration, note that the json_config and dict_config should not be
         used both, you only need to take one ot them.
        :param json_config: A Json string contains the params specified by user
        :param dict_config: A dict contains the params specified by user
        :return:
        """
        self.update(dict_config)
        if dict_config == None:
            print('dict_config is none')
        else:
            print('dict_config is not none')
        self.dict_to_jconf()

    def json_to_jconf(self, json_config):
        """
        Transfer a json format string to Configuration.
        :param json_config: json format string
        :return: Configuration dict which contains the params json has.
        """
        parsed_json = json.loads(json_config)
        appended_config = dict(Counter(json_config)+Counter(self))
        jmap = Configuration._jvm.java.util.HashMap()
        for k, v in appended_config:
            jmap[k] = v
        return self._jvm.com.tencent.angel.api.python.PythonUtils.addMapToConf(jmap, Configuration._jconf)

    def dict_to_jmap(self):
        """
        Transfer a python dict(self) to a Java HashMap
        :return: A HashMap which contains all params user has set using Configuration as a dict.
        """
        jmap = Configuration._jvm.java.util.HashMap()
        for k, v in self.items():
            jmap[k] = v
        return jmap

    def dict_to_jconf(self):
        """
        Transfer a python config instance(a dict indeed) to com.tencent.angel.client.AngelClientFactory,
        then get a hadoop Configuration instance which contains all the property set by python code.
        :return: Hadoop Configuration instance
        """
        jmap = self.dict_to_jmap()
        return self._jvm.com.tencent.angel.api.python.PythonUtils.addMapToConf(jmap, Configuration._jconf)

    def set(self, name, value):
        """Set a configuration property."""
        if self._jconf is not None:
            self._jconf.set(name, str(value))
        else:
            self[name] = str(value)
        return self

    def set_boolean(self, name, value):
        self._jconf.setBoolean(name, value)
        return self

    def set_int(self, name, value):
        self._jconf.setInt(name, value)
        return self

    def get(self, name):
        return self._jconf.get(name)

    def get(self, name, default_value):
        return self._jconf.get(name, default_value)

    def get_int(self, name):
       return self._jconf.getInt(name)

    def get_int(self, name, default_value):
       return self._jconf.getInt(name, default_value)
