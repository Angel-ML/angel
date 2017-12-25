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

from __future__ import print_function
import os
import sys


def _find_angel_home():
    """Find the ANGEL_HOME."""
    # If the enviroment has ANGEL_HOME set trust it.
    if "ANGEL_HOME" in os.environ:
        return os.environ["ANGEL_HOME"]

    def is_angel_home(path):
        """Takes a path and returns true if the provided path could be a reasonable ANGEL_HOME"""
        return (os.path.isfile(os.path.join(path, "bin/angel-submit")) and
                (os.path.isdir(os.path.join(path, "jars")) or
                 os.path.isdir(os.path.join(path, "assembly"))))

    paths = ["../", os.path.dirname(os.path.realpath(__file__))]

    # Add the path of the PyAngel module if it exists
    if sys.version < "3":
        import imp
        try:
            module_home = imp.find_module("pyangel")[1]
            paths.append(module_home)
            # If we are installed in edit mode also look two dirs up
            paths.append(os.path.join(module_home, "../../"))
        except ImportError:
            # Not pip installed no worries
            pass
    else:
        from importlib.util import find_spec
        try:
            module_home = os.path.dirname(find_spec("pyangel").origin)
            paths.append(module_home)
            # If we are installed in edit mode also look two dirs up
            paths.append(os.path.join(module_home, "../../"))
        except ImportError:
            # Not pip installed no worries
            pass

    # Normalize the paths
    paths = [os.path.abspath(p) for p in paths]

    try:
        return next(path for path in paths if is_angel_home(path))
    except StopIteration:
        print("Could not find valid ANGEL_HOME while searching {0}".format(paths), file=sys.stderr)
        exit(-1)

if __name__ == "__main__":
    print(_find_angel_home())
