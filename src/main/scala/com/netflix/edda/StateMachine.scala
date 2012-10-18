package com.netflix.edda

import scala.actors.Actor
import org.slf4j.{ Logger, LoggerFactory }

import java.util.concurrent.Callable

import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge

object StateMachine {
  type State = Map[String, Any]
  trait Message {
    def from: Actor
  }
  case class Stop(from: Actor) extends Message {}

  trait ErrorMessage extends Message {}
  case class InvalidMessageError(from: Actor, reason: String, message: Any) extends ErrorMessage;
  case class UnknownMessageError(from: Actor, reason: String, message: Any) extends ErrorMessage;

  class LocalState[T] {
    def localStateKey = this.getClass.getName
    def newLocalState(init: T) = (localStateKey -> init)
    def setLocalState(state: StateMachine.State, localState: T) = state + (localStateKey -> localState)
    def localState(state: StateMachine.State): T = state.get(localStateKey) match {
      case Some(localState) => localState.asInstanceOf[T]
      case other => throw new java.lang.RuntimeException(localStateKey + " state missing from current state")
    }
  }
}

class StateMachine extends Actor {
  import StateMachine._
  private[this] val logger = LoggerFactory.getLogger(getClass)

  protected def init: Unit = {}

  protected def initState: State = Map()

  def stop() {
    this ! Stop(this)
  }

  protected def addInitialState(state: State, stateTup: (String, Any)): State = {
    val (localStateKey, initValue) = stateTup
    state isDefinedAt localStateKey match {
      case true => throw new java.lang.RuntimeException("State for " + localStateKey + " already initialized")
      case false => state + (localStateKey -> initValue)
    }
  }

  protected def transitions: PartialFunction[(Message, State), State] = {
      case (UnknownMessageError(from,reason,message),state) => 
          throw new java.lang.RuntimeException(reason)
      case (InvalidMessageError(from,reason,message),state) => 
          throw new java.lang.RuntimeException(reason)
  }

  private[this] val self = this
  protected val mailboxSizeGauge = new BasicGauge[java.lang.Long](
    MonitorConfig.builder("mailboxSize").build(),
    new Callable[java.lang.Long] {
      def call() = self.mailboxSize
    })

  final def act() {
    init
    var state = initState
    var keepLooping = true
    loopWhile(keepLooping) {
      react {
        case Stop(from) => {
          keepLooping = false
        }
        case message: Message => {
          if (!transitions.isDefinedAt(message, state)) {
            logger.error("Unknown Message " + message + " sent from " + sender)
            sender ! UnknownMessageError(this, "Unknown Message " + message, message)
          }
          logger.debug(sender + ": " + message + " -> " + this)
          try {
            state = transitions(message, state)
          } catch {
            case e => {
              logger.error("failed to handle event " + message, e)
            }
          }
        }
        case message => {
          logger.error("Invalid Message " + message + " sent from " + sender)
          sender ! InvalidMessageError(this, "Invalid Message " + message, message)
        }
      }
    }
  }
}
