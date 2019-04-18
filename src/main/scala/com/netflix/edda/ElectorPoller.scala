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

import scala.actors.Actor
import scala.actors.TIMEOUT

import org.slf4j.LoggerFactory

import org.joda.time.DateTime

class ElectorPoller(elector: Elector) extends Actor {
  val logger = LoggerFactory.getLogger(getClass)

  override def toString = elector + " poller"

  override def act() = {
    var keepLooping = true
    Actor.self.loopWhile(keepLooping) {
      implicit val req = RequestId(Utils.uuid + " poller")
      Actor.self.reactWithin(elector.pollCycle.get.toInt) {
        case got @ StateMachine.Stop(from) => keepLooping = false
        case got @ TIMEOUT => {
          if (logger.isDebugEnabled) logger.debug(s"$req$this received: $got")
          val msg = Elector.RunElection(Actor.self)
          if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $elector")
          elector ! msg
        }
      }
    }
  }

  override def exceptionHandler = {
    case e: Exception => if (logger.isErrorEnabled) logger.error(this + " failed to setup election poller", e)
  }
  
  def stop()(implicit req: RequestId) {
    val msg = StateMachine.Stop(this)
    if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $this")
    this ! msg
  }

}
