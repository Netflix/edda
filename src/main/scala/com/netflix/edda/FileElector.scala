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
