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


"""
An interactive shell.

This file is designed to be launched as a PYTHONSTARTUP script.
"""
import platform
import tempfile

from hadoop.local_fs import LocalFileSystem

from pyangel.context import Configuration
from pyangel.ml.conf import MLConf
from pyangel.conf import AngelConf

try:
    Configuration._init()
except RuntimeError:
    print("Oops!PyAngel failed to initialize")


# To Do
# Modify the way to get current Angel version


conf = Configuration()
conf[AngelConf.ANGEL_INPUTFORMAT_CLASS] = 'org.apache.hadoop.mapreduce.lib.input.CombineTextInputFormat'
conf.set_boolean("mapred.mapper.new-api", True)
conf.set_boolean(AngelConf.ANGEL_JOB_OUTPUT_PATH_DELETEONEXIST, True)

LOCAL_FS = LocalFileSystem.DEFAULT_FS
TMP_PATH = tempfile.gettempdir()

conf[AngelConf.ANGEL_DEPLOY_MODE] = "LOCAL"
conf[AngelConf.ANGEL_SAVE_MODEL_PATH] = LOCAL_FS + TMP_PATH + "/out"
conf[AngelConf.ANGEL_LOG_PATH] = LOCAL_FS + TMP_PATH + "/log"
conf[AngelConf.ANGEL_WORKERGROUP_NUMBER] = 1
conf[AngelConf.ANGEL_WORKER_TASK_NUMBER] = 1
conf[AngelConf.ANGEL_PS_NUMBER] = 1


print("""Welcome to
   ___                    __
  / _ |  ___  ___ _ ___  / /
 / __ | / _ \/ _ `// -_)/ / 
/_/ |_|/_//_/\_, / \__//_/   version %s
            /___/           
""" % "1.3.0")

print("Using Python version %s (%s, %s)" % (
    platform.python_version(),
    platform.python_build()[0],
    platform.python_build()[1]))
print("Configuration available as 'conf'.")
print("MLConf available as 'MLConf'.")
print("AngelConf available as 'AngelConf'.")
print("Notice that default deploy mode is 'Local', if you want to use yarn mode,")
print("use: conf[AngelConf.ANGEL_DEPLOY_MODE] = \"YARN\" to overwrite.")
