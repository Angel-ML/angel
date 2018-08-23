#!/usr/bin/env bash
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


FIND_ANGEL_HOME_PYTHON_SCRIPT="$(cd "$(dirname "$0")"; pwd)/find_angel_home.py"

if [ ! -z "${ANGEL_HOME}" ]; then
  exit 0
elif [ ! -f "$FIND_ANGEL_HOME_PYTHON_SCRIPT" ]; then
  ANGEL_HOME="$(cd "$(dirname "$0")"/..; pwd)"
  export ANGEL_HOME
else
  if [[ -z "$PYANGEL_PYTHON_SHELL" ]]; then
    PYANGEL_PYTHON_SHELL="${PYANGEL_PYTHON:-"python"}"
  fi
  export ANGEL_HOME="$(cd "$(dirname "$0")")"
fi
