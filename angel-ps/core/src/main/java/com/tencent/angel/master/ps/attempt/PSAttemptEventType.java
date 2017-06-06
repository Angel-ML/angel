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

package com.tencent.angel.master.ps.attempt;

/**
 * PS attempt event type.
 */
public enum PSAttemptEventType {
  /**register to master*/
  PA_REGISTER, 
  
  /**assign a container*/
  PA_CONTAINER_ASSIGNED, 
  
  /**container is launched successfully*/
  PA_CONTAINER_LAUNCHED, 
  
  /**container is launched failed*/
  PA_CONTAINER_LAUNCH_FAILED, 
  
  /**request resource*/
  PA_SCHEDULE, 
  
  /**update state*/
  PA_UPDATE_STATE, 
  
  /**unregister from master*/
  PA_UNREGISTER, 
  
  /**run failed*/
  PA_FAILMSG, 
  
  /**killed*/
  PA_KILL, 
  
  /**notify the ps attempt to save matrices*/
  PA_COMMIT,
  
  /**sava matrices failed*/
  PA_COMMIT_FAILED, 
  
  /**save matrices successfully*/
  PA_COMMIT_SUCCESS, 
  
  /**run over successfully*/
  PA_SUCCESS, 
  
  /**the container is exited*/
  PA_CONTAINER_COMPLETE, 
  
  /**diagnostices update*/
  PA_DIAGNOSTICS_UPDATE
}
