/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import scala.concurrent.ExecutionContext.Implicits.global

import org.slf4j.LoggerFactory

/** state for Elector StateMachine
  *
  * @param isLeader boolean flag to indicate if we are leader
  */
case class ElectorState(isLeader: Boolean = false)

/** companion object for [[com.netflix.edda.Elector]]. */
object Elector extends StateMachine.LocalState[ElectorState] {

  val logger = LoggerFactory.getLogger(getClass)

  /** Message sent to observers after an Election */
  case class ElectionResult(from: Actor, result: Boolean) extends StateMachine.Message

  /** Message to start an election and decide leadership */
  case class RunElection(from: Actor) extends StateMachine.Message

  /** message to get the leadership status */
  case class IsLeader(from: Actor) extends StateMachine.Message
}

/** interface for determining leadership in a multi system configuration.
  */

abstract class Elector extends Observable {

  import Elector._
  import Utils._

  /** synchronous call to the StateMachine to fetch the leadership status */
  def isLeader: Boolean = {
    val msg = IsLeader(Actor.self)
    if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + this + " with 10000ms timeout")
    this !?(10000, msg) match {
      case Some(ElectionResult(from, result)) => result
      case Some(message) => throw new java.lang.UnsupportedOperationException("Failed to determine leadership: " + message)
      case None => throw new java.lang.RuntimeException("TIMEOUT: isLeader response within 10s")
    }
  }

  val pollCycle = Utils.getProperty("edda.elector", "refresh", "", "10000")

  /** abstract method to determine leadership */
  protected def runElection(): Boolean

  /** setup initial Elector state */
  protected override def initState = addInitialState(super.initState, newLocalState(ElectorState()))

  /** set up observers to watch ourselves for leadership results.  Start an election immediately
    * then start the poller that will periodically run elections. */
  protected override def init() {
    Utils.NamedActor(this + " init") {
      val msg = RunElection(Actor.self)
      if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + this)
      this ! msg
      electionPoller()
      super.init
      // listen to our own message events
      def retry: Unit = {
        this.addObserver(this) onFailure {
          case msg => {
            if (logger.isErrorEnabled) logger.error(Actor.self + "failed to add observer " + this + " to " + this + ", retrying")
            retry
          }
        }
      }
      retry
    }
  }

  /** run an election periodically */
  protected def electionPoller() {
    Utils.NamedActor(this + " poller") {
      Actor.self.loop {
        Actor.self.reactWithin(pollCycle.get.toInt) {
          case got @ TIMEOUT => {
            if (logger.isDebugEnabled) logger.debug(Actor.self + " received: " + got)
            val msg = RunElection(Actor.self)
            if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + this)
            this ! msg
          }
        }
      }
    }.addExceptionHandler({
      case e: Exception => if (logger.isErrorEnabled) logger.error(this + " failed to refresh", e)
    })
  }

  /** handle StateMachine messages */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (RunElection(from), state) => {
      flushMessages {
        case RunElection(from) => true
      }
      // Utils.NamedActor(this + " election runner") {
      scala.concurrent.future {
        val result = runElection()
        val msg = ElectionResult(this, result)
        Observable.localState(state).observers.foreach(o => {
            if (logger.isDebugEnabled) logger.debug(this + " sending: " + msg + " -> " + o)
            o ! msg
        })
      }
      state
    }
    case (ElectionResult(from, result), state) => {
      setLocalState(state, ElectorState(result))
    }
    case (IsLeader(from), state) => {
      val msg = ElectionResult(this, localState(state).isLeader)
      if (logger.isDebugEnabled) logger.debug(this + " sending: " + msg + " -> " + sender)
      sender ! msg
      state
    }
  }

  override def transitions = localTransitions orElse super.transitions
}

