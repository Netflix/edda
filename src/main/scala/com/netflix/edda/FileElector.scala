/*
 * Copyright 2012-2019 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda

import org.slf4j.LoggerFactory
class FileElector extends Elector {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  
  protected override def runElection()(implicit req: RequestId): Boolean = {
    val file = new java.io.File("/tmp/eddaLeader")
    val isLeader = if( file.exists ) {
      true
    } else {
      logger.warn(s"$req /tmp/eddaLeader file does not exist, not becoming leader")
      false
    }
    logger.info(s"$req Leader: $isLeader [/tmp/eddaLeader]")
    isLeader
  }
}
