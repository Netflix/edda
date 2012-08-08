package com.netflix.edda

import scala.actors.Actor

object StateMachine {
    type State = Map[String,Any]
    trait Message {}
    
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
                        throw new java.lang.UnsupportedOperationException("Unknown Message " + message)
                    }
                    state = transitions(message,state)
                }
            }
        }
    }
}
