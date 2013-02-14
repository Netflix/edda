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
  *
  * @param ctx configuration context
  */

abstract class Elector(ctx: ConfigContext) extends Observable {

  import Elector._

  /** synchronous call to the StateMachine to fetch the leadership status */
  def isLeader: Boolean = {
    val self = this
    val msg = IsLeader(this)
    logger.debug(this + " sending: " + msg + " -> " + this + " with 10000ms timeout")
    this !?(10000, msg) match {
      case Some(ElectionResult(`self`, result)) => result
      case Some(message) => throw new java.lang.UnsupportedOperationException("Failed to determine leadership: " + message)
      case None => throw new java.lang.RuntimeException("TIMEOUT: isLeader response within 10s")
    }
  }

  val pollCycle = ctx.config.getProperty("edda.elector.refresh", "10000").toInt

  /** abstract method to determine leadership */
  protected def runElection(): Boolean

  /** setup initial Elector state */
  protected override def initState = addInitialState(super.initState, newLocalState(ElectorState()))

  /** set up observers to watch ourselves for leadership results.  Start an election immediately
    * then start the poller that will periodically run elections. */
  protected override def init() {
    // it is a sync call so put it in another thread
    Actor.actor {
      this.addObserver(this)
    }
    val msg = RunElection(this)
    logger.debug(this + " sending: " + msg + " -> " + this)
    this ! msg
    electionPoller()
  }

  /** run an election periodically */
  protected def electionPoller() {
    val elector = this
    Utils.NamedActor(this + " poller") {
      Actor.loop {
        Actor.reactWithin(pollCycle) {
          case TIMEOUT => {
            val msg = RunElection(this)
            logger.debug(this + " sending: " + msg + " -> " + elector)
            elector ! msg
          }
        }
      }
    }.addExceptionHandler({
      case e: Exception => logger.error(this + " failed to refresh", e)
    })
  }

  /** handle StateMachine messages */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (RunElection(from), state) => {
      Utils.NamedActor(this + " election runner") {
        val result = runElection()
        val msg = ElectionResult(this, result)
        Observable.localState(state).observers.foreach(o => {
            logger.debug(this + " sending: " + msg + " -> " + o)
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
      logger.debug(this + " sending: " + msg + " -> " + sender)
      sender ! msg
      state
    }
  }

  override def transitions = localTransitions orElse super.transitions
}

