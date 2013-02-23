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

/** local state for StateMachine */
case class ObservableState(observers: List[Actor] = List[Actor]())

/** companion for [[com.netflix.edda.Observable]], contains messages for StateMachine */
object Observable extends StateMachine.LocalState[ObservableState] {

  /** Message to add observer to local state */
  case class Observe(from: Actor, actor: Actor) extends StateMachine.Message

  /** Message to remove observer from local state */
  case class Ignore(from: Actor, actor: Actor) extends StateMachine.Message

  /** Response to use for sync calls to add/remove observers */
  case class OK(from: Actor) extends StateMachine.Message

}

/** register or un-register observers with various StateMachine objects.  This is needed if
  * one StateMachine needs to receive events (state changes) from another StateMachine.  For
  * example a Collection is registered as an observer of a Crawler so
  * that the Collection gets updates when the Crawler state changes (so that the Collection
  * can persist the changes to the DataStore).
  */
abstract class Observable extends StateMachine {

  import Observable._
  import Utils._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  //* notify the given actor when the state changes */
  def addObserver(actor: Actor)(events: EventHandlers = DefaultEventHandlers): Nothing = {
    val msg = Observe(Actor.self, actor)
    logger.debug(Actor.self + " sending: " + msg + " -> " + this + " with 60s timeout")
    this ! msg
    Actor.self.reactWithin(60000) {
      case msg: OK => {
        logger.debug(Actor.self + " received: " + msg + " from " + sender)
        events(Success(msg))
      }
      case msg @ TIMEOUT => {
        logger.debug(Actor.self + " received: " + msg)
        events(Failure((msg, 60000)))
      }
    }
  }

  //* stop notifying the give actor when the state changes */
  def delObserver(actor: Actor)(events: EventHandlers = DefaultEventHandlers): Nothing = {
    val msg = Ignore(Actor.self, actor)
    logger.debug(Actor.self + " sending: " + msg + " -> " + this + " with 60s timeout")
    this ! msg
    Actor.self.reactWithin(60000) {
      case msg: OK => {
        logger.debug(Actor.self + " received: " + msg + " from " + sender)
        events(Success(msg))
      }
      case msg @ TIMEOUT => {
        logger.debug(Actor.self + " received: " + msg)
        events(Failure((msg, 60000)))
      }
    }
  }

  protected override def initState = addInitialState(super.initState, newLocalState(ObservableState()))

  /** setup trasitions to handle Oberserve and Ignore messages */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Observe(from, caller), state) => {
      val msg = OK(this)
      logger.debug(this + " sending: " + msg + " -> " + sender)
      sender ! msg
      setLocalState(state, ObservableState(caller :: localState(state).observers))
    }
    case (Ignore(from, caller), state) => {
      val msg = OK(this)
      logger.debug(this + " sending: " + msg + " -> " + sender)
      sender ! msg
      setLocalState(state, ObservableState(localState(state).observers diff List(caller)))
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}
