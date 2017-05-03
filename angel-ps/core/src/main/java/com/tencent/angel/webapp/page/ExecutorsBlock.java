/*
 * Tencent is pleased to support the open source community by making Angel available.
 *
 * Copyright (C) 2017 THL A29 Limited, a Tencent company. All rights reserved.
 *
 * Licensed under the BSD 3-Clause License (the "License"); you may not use this file except in
 * compliance with the License. You may obtain a copy of the License at
 *
 * https://opensource.org/licenses/BSD-3-Clause
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is
 * distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.tencent.angel.webapp.page;

import com.google.inject.Inject;
import com.tencent.angel.master.app.AMContext;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TABLE;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.TBODY;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;

import static org.apache.hadoop.yarn.util.StringHelper.join;
import static org.apache.hadoop.yarn.webapp.view.JQueryUI._TH;

public class ExecutorsBlock extends HtmlBlock {
  final AMContext amContext;

  @Inject
  ExecutorsBlock(AMContext amctx) {
    amContext = amctx;
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel ExecutorsBlock"));
    TBODY<TABLE<Hamlet>> tbody =
        html.h1("ExecutorsBlock").table("#jobs").thead().tr().th(_TH, "id").th(_TH, "name")
            .th(_TH, "state").th(_TH, "stacktrace")._()._().tbody();

    ThreadMXBean threadMXBean = ManagementFactory.getThreadMXBean();
    ThreadInfo[] threadInfo = threadMXBean.dumpAllThreads(true, true);
    String stackTraceString;
    for (ThreadInfo t : threadInfo) {
      stackTraceString = "";
      StackTraceElement[] stackTrace = t.getStackTrace();
      for (StackTraceElement s : stackTrace) {
        stackTraceString += s.toString() + "\n";
      }
      tbody.tr().td(String.valueOf(t.getThreadId())).td(String.valueOf(t.getThreadName()))
          .td(String.valueOf(t.getThreadState())).td(String.valueOf(stackTraceString))._();
    }
    tbody._()._();


  }


}
