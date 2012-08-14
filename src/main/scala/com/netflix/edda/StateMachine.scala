package com.netflix.edda

import scala.actors.Actor
import org.slf4j.{Logger, LoggerFactory}

object StateMachine {
    type State = Map[String,Any]
    trait Message {}
    trait ErrorMessage extends Message {}
    
    case class InvalidMessageError(reason: String, message: Any) extends ErrorMessage;
    case class UnknownMessageError(reason: String, message: Any) extends ErrorMessage;
    
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

    start

    protected
    def init: Unit = {}
    
    protected
    def initState: State = Map()

    protected
    def addInitialState(state: State, stateTup: (String,Any)): State = {
        val (localStateKey,initValue) = stateTup
        state isDefinedAt localStateKey match {
            case true => throw new java.lang.RuntimeException("State for " + localStateKey + " already initialized")
            case false => state + (localStateKey -> initValue)
        }
    }

    protected
    def transitions: PartialFunction[(Message,State), State] = Map()

    final 
    def act() {
        init
        var state = initState
        loop {
            react {
                case message: Message =>  {
                    if( ! transitions.isDefinedAt(message,state) ) {
                        logger.error("Unknown Message " + message + " sent from " + sender)
                        sender ! UnknownMessageError("Unknown Message " + message, message)
                    }
                    state = transitions(message,state)
                }
                case message => {
                    logger.error("Invalid Message " + message + " sent from " + sender)
                    sender ! InvalidMessageError("Invalid Message " + message, message)
                }
            }
        }
    }
}
