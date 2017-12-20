#
# Tencent is pleased to support the open source community by making Angel available.
#
# Copyright (C) 2017 THL A29 Limited] = a Tencent company. All rights reserved.
#
# Licensed under the BSD 3-Clause License (the "License") you may not use this file except in
# compliance with the License. You may obtain a copy of the License at
#
# https:#opensource.org/licenses/BSD-3-Clause
#
# Unless required by applicable law or agreed to in writing] = software distributed under the License is
# distributed on an "AS IS" BASIS] = WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
# either express or implied. See the License for the specific language governing permissions and
#

class PSModel(object):

    def __init__(self,conf, model_name, row, col, block_row = -1, block_col = -1, need_save = True):
        self._conf = conf
        self._jvm = conf._jconf._jvm
        self._jmodel = self._jvm.com.tencent.angel.ml.model.PSModel

    def get_task_context(self):
        return self._jmodel.getTaskContext()

    def get_context(self):
        return self._jmodel.getContext()

    def get_client(self, model_name):
        return self._jmodel.getMatrix(model_name)

    def get_matrix_id(self):
        return self._jmodel.getMatrixId()

    def set_need_save(self, need_save):
        return self._jmodel.setNeedSave(need_save)

    def set_attribute(self, key, value):
        return self._jmodel.setAttribute(key, value)

    def set_average(self, average):
        return self._jmodel.setAverage(average)

    def set_hogwild(self, hogwild):
        return  self._jmodel.setHogwild(hogwild)

    def set_oplog_type(self, oplog_type):
        return self._jmodel.setOplogType(oplog_type)

    def set_row_type(self, row_tyle):
        return self._jmodel.setRowType(row_tyle)

    def set_load_path(self, path):
        return self._jmodel.setLoadPath(path)

    def set_save_path(self, path):
        return self._jmodel.setSavePath(path)

    def sync_clock(self, flush = True):
        return self._jmodel.syncClock(flush)

    def increment(self, delta):
        return self._jmodel.incremetn(delta)

    def increment(self, row_index, delta):
        # To do: need python edition of TVector
        return self._jmodel.increment(row_index, delta)

    def incremnent(self, deltas):
        return self._jmodel.increment(deltas)

    def get(self, func):
        # To do: need python edition of GetFunc
        return self._jmodel.get(func)

    def get_row(self, row_index):
        return self._jmodel.getRow(row_index)

    def get_rows(self, row_index, batch_num):
        return self._jmodel.getRows(row_index, batch_num)

    def get_rows(self, row_index):
        return self._jmodel.getRows(row_index)

    def get_rows_flow(self, row_index, batch_num):
        return self._jmodel.getRowsFlow(row_index, batch_num)

    def get_value_of_indexs(self, row_id, index):
        return self._jmodel.getValueOfIndexes(row_id, index)

    def update(self, update_func):
        return self._jmodel.update(update_func)

    def zero(self):
        return self._jmodel.zero()


