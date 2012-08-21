package com.netflix.edda

import scala.actors.Actor

import org.slf4j.{Logger, LoggerFactory}

case class ObservableState(observers: List[Actor] = List[Actor]())

object Observable extends StateMachine.LocalState[ObservableState] {
    // internal messages
    private case class Observe(actor: Actor) extends StateMachine.Message
    private case class Ignore(actor: Actor)  extends StateMachine.Message
    private case class OK()                  extends StateMachine.Message
}

abstract class Observable extends StateMachine {
    import Observable._
    private[this] val logger = LoggerFactory.getLogger(getClass)
    
    def addObserver(actor: Actor) {
        this !? Observe(actor) match {
            case OK() =>
            case message => throw new java.lang.UnsupportedOperationException("Failed to add observer " + message);
        }
    }

    def delObserver(actor: Actor) {
        this !? Ignore(actor) match {
            case OK() =>
            case message => throw new java.lang.UnsupportedOperationException("Failed to remove observer " + message);
        }
    }

    protected override 
    def initState = addInitialState(super.initState, newLocalState(ObservableState()))

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Observe(caller),state) => {
            sender ! OK()
            setLocalState(state, ObservableState(caller :: localState(state).observers))
        }
        case (Ignore(caller),state) => {
            sender ! OK()
            setLocalState(state, ObservableState(localState(state).observers diff List(caller)))
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}
