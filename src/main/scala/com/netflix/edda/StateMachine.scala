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
import org.slf4j.LoggerFactory

import java.util.concurrent.Callable

import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge

/** companion for [[com.netflix.edda.StateMachine]] holding base message type for
  * all state machine transition messages.
  */
object StateMachine {

  /** basic state for each StateMachine.  The String will be the name of the class
    * in the inheritance chain and the value will be whatever class that object
    * wants to store for state (usually a case class) */
  type State = Map[String, Any]

  /** all messages to a StateMachine need to extend Message */
  trait Message {
    def from: Actor
  }

  /** message to Stop the StateMachine */
  case class Stop(from: Actor) extends Message {}

  /** trait to determin if message is an error type */
  trait ErrorMessage extends Message {}

  /** sent in case the message does not extend Message trait */
  case class InvalidMessageError(from: Actor, reason: String, message: Any) extends ErrorMessage

  /** sent in the case there are no matching case clauses for the the incoming message */
  case class UnknownMessageError(from: Actor, reason: String, message: Any) extends ErrorMessage
  
  /** keep track of a local state for each subclass of the StateMachine. For the inheritance of
    * Collection->Queryable->Observable->StateMachine we could have separate states
    * for Collection, Queryable, and Observable. (in this case Queryable has no internal state)
    * The LocalState routines are imported typically via a companion object like:
    * {{{
    * object Collection extends StateMachine.LocalState[CollectionState] {...}
    * class Collection {
    *   import Collection._
    *   protected override def initState =
    *     addInitialState(super.initState, newLocalState(CollectionState(...)))
    * }
    * }}}
    */
  class LocalState[T] {
    def localStateKey = this.getClass.getName

    /** should be called from from StateMachine.initState to initialize a new local state */
    def newLocalState(init: T) = (localStateKey -> init)

    /** update local state for your StateMachine
      * {{{
      *   setLocalState(state, localState(state).copy(crawled = newRecords))
      * }}}
      */
    def setLocalState(state: StateMachine.State, localState: T) = state + (localStateKey -> localState)

    /** get the state for your local class */
    def localState(state: StateMachine.State): T = state.get(localStateKey) match {
      case Some(localState) => localState.asInstanceOf[T]
      case other => throw new java.lang.RuntimeException(localStateKey + " state missing from current state")
    }
  }

}

/** Base class for our state machine.  The state is stored in immutable data, so we will always have
  * a consistent state.
  */
class StateMachine extends Actor {

  import StateMachine._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  protected def init() {
    val msg = 'INIT
    logger.debug(Actor.self + " sending: " + msg + " -> " + this)
    this ! msg
  }

  /** subclasses need to overload this routine when local state is required:
    * {{{
    *   protected override def initState = addInitialState(super.initState, newLocalState(CollectionState(records = load(replicaOk = false))))
    * }}}
    * @return
    */
  protected def initState: State = Map()

  /** stop the state machine */
  def stop() {
    val msg = Stop(this)
    logger.debug(Actor.self + " sending: " + msg + " -> " + this)
    this ! msg
  }

  /** used from subclasses initState routine to add their localState object to the overall StateMachine state */
  protected def addInitialState(state: State, stateTup: (String, Any)): State = {
    val (localStateKey, initValue) = stateTup
    state isDefinedAt localStateKey match {
      case true => throw new java.lang.RuntimeException("State for " + localStateKey + " already initialized")
      case false => state + (localStateKey -> initValue)
    }
  }

  /** PartialFunction to allow Messages to transition the state machine from one state to another.  Subclasses
    * must override this routine to handle new messages types.
    * {{{
    *   private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    *     case (NewMessage,state) => setLocalState(state, localState(state).copy(value=newValue))
    *   }
    *   override protected def transitions = localTransitions orElse super.transitions
    * }}}
    * @return
    */
  protected def transitions: PartialFunction[(Message, State), State] = {
    case (UnknownMessageError(from, reason, message), state) =>
      throw new java.lang.RuntimeException(reason)
    case (InvalidMessageError(from, reason, message), state) =>
      throw new java.lang.RuntimeException(reason)
  }

  private[this] val self = this
  protected val mailboxSizeGauge = new BasicGauge[java.lang.Long](
    MonitorConfig.builder("mailboxSize").build(),
    new Callable[java.lang.Long] {
      def call() = {
        logger.info(self + " mailboxSize: " + self.mailboxSize)
        self.mailboxSize
      }
    })

  /** the main loop for the StateMachine actor.  It will call initState then start looping
    * and react'ing to messages until it gets a Stop message. */
  final def act() {
    logger.debug(this + " before init")
    init()
    logger.debug(this + " after init")
    Actor.self.react { 
      case msg @ 'INIT => {
        logger.debug(this + " received: " + msg + " from " + sender)
        var state = initState
        var keepLooping = true
        Actor.self.loopWhile(keepLooping) {
          Actor.self.react {
            case msg @ Stop(from) => {
              logger.debug(this + " received: " + msg + " from " + sender)
              keepLooping = false
            }
            case message: Message => {
              if (!transitions.isDefinedAt(message, state)) {
                logger.error("Unknown Message " + message + " sent from " + sender)
                val msg = UnknownMessageError(this, "Unknown Message " + message, message) 
                logger.debug(this + " sending: " + msg + " -> " + sender)
                sender ! msg
              }
              logger.debug(this + " received: " + message + " from " + sender)
              try {
                state = transitions(message, state)
              } catch {
                case e: Exception => {
                  logger.error("failed to handle event " + message, e)
                }
              }
            }
            case message => {
              logger.error("Invalid Message " + message + " sent from " + sender)
              val msg = InvalidMessageError(this, "Invalid Message " + message, message) 
              logger.debug(this + " sending: " + msg + " -> " + sender)
              sender ! msg
            }
          }
        }
      }
    }
  }

  var handlers: PartialFunction[Exception,Unit] = {
    case e: Exception => logger.error(this + " caught exception", e)
  }
    
  /** add a partial function to allow for specific exception
    * handling when needed
    * @param pf PartialFunction to handle exception types
    */
  def addExceptionHandler(pf: PartialFunction[Exception,Unit]) {
    handlers = pf orElse handlers 
  }

  /** setup exceptionHandler to use the custom handlers modified
    * with addExceptionHandler
    */
  override def exceptionHandler = handlers

}
