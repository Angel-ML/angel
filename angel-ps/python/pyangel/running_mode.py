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

from enum import Enum, unique

@unique
class RunningMode(Enum):
    """
    Enum for running mode
    """
    # Run ParameterServer  & ParameterServerAgent
    ANGEL_PS_PSAGENT = 0

    # Only Run ParameterServer
    ANGEL_PS = 1

    # Run ParameterServer & Worker(embedded ParameterServerAgent)
    ANGEL_PS_WORKER = 2
