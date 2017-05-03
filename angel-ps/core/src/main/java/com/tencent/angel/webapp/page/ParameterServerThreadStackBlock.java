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
import com.google.protobuf.ServiceException;
import com.tencent.angel.exception.UnvalidIdStrException;
import com.tencent.angel.master.app.AMContext;
import com.tencent.angel.master.psclient.PSClient;
import com.tencent.angel.ps.PSAttemptId;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.yarn.webapp.view.HtmlBlock;

import java.io.IOException;

import static com.tencent.angel.webapp.AngelParams.PSATTEMPT_ID;

import static org.apache.hadoop.yarn.util.StringHelper.join;


public class ParameterServerThreadStackBlock extends HtmlBlock {
  final AMContext amContext;
  private static final Log LOG = LogFactory.getLog(ParameterServerThreadStackBlock.class);

  @Inject
  ParameterServerThreadStackBlock(AMContext amctx) {
    amContext = amctx;
    amContext.getParameterServerManager();
  }

  @Override
  protected void render(Block html) {
    set(TITLE, join("Angel ParameterServerThread ", $(PSATTEMPT_ID)));
    PSAttemptId psAttempttId = null;
    try {
      psAttempttId = new PSAttemptId($(PSATTEMPT_ID));
    } catch (UnvalidIdStrException e) {
      LOG.error("unvalid id string, ", e);
      return;
    }

    try {
      LOG.info("start init PSClient");
      PSClient psClient = new PSClient(amContext, psAttempttId);
      String info = psClient.getThreadStack();
      html.pre()._(info)._();
    } catch (IOException | ServiceException e) {
      LOG.error("get thread stack from ps " + psAttempttId + " failed. ", e);
    }
  }

}
