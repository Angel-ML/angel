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

from __future__ import print_function
import glob
import os
import sys
from setuptools import setup, find_packages

if sys.version_info < (2.7):
    print("PyAngel not support prior to 2.7 when you use pip.",
          file=sys.stderr)
    exit(-1)

try:
    exec(open('pyangel/version.py').read())
except IOError:
    print("Failed to load PyAngel version file for installing. You operation should be in Angel's python directory",
          file=sys.stderr)
    sys.exit(-1)

VERSION = __version__

TEMP_PATH = "deps"
ANGEL_HOME = os.path.abspath("../")

incorrect_invocation_message = """
If you are installing pyangel from angel source, you must first build Angel and run sdist.

    To build Angel with maven you can run:
        mvn -DskipTests clean package
    Building the source dist is done in the python directoty:
        cd python
        python setup.py sdist
        pip install dist/*.tar.gz
"""

JARS_PATH = glob.glob(os.path.join(ANGEL_HOME, "lib/"))

if len(JARS_PATH) == 1:
    JARS_PATH = JARS_PATH[0]
elif (os.path.isfile("../RELEASE") and len(glob.glob("../lib/angel*ps*core*.jar")) == 1):
    JARS_PATH = os.path.join(ANGEL_HOME, "jars")
elif len(JARS_PATH) == 0 and not os.path.exists(TEMP_PATH):
    print(incorrect_invocation_message, file=sys.stderr)
    sys.exit(-1)

EXAMPLES_PATH = os.path.join(ANGEL_HOME, "python/examples")
SCRIPT_PATH = os.path.join(ANGEL_HOME, "bin")
DATA_PATH = os.path.join(ANGEL_HOME, "data")

SCRIPTS_TARGET = os.path.join(TEMP_PATH, "bin")

script_names = os.listdir(SCRIPTS_TARGET)
scripts = list(map(lambda script: os.path.join(SCRIPTS_TARGET, script), script_names))
setup(
    name='pyangel',
    version=VERSION,
    description='Tencent Angerl Python API',
    author='Angel Deveplopers',
    url='https://github.com/tencent/angel/tree/master/python',
    packages=['pyangel',
              'pyangel.ml',
              'pyangel.ml.classification',
              'pyangel.ml.client',
              'pyangel.ml.clustering',
              'pyangel.ml.factorizationmachines',
              'pyangel.ml.gbdt',
              'pyangel.ml.lda',
              'pyangel.ml.matrixfactorization',
              'pyangel.ml.model',
              'pyangel.ml.regression'],
    include_pachages_data=True,
    scripts=scripts,
    install_requires=['py4j==0.10.4'],
    classifiers=[
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 2.7']
)
