package com.netflix.edda

import scala.actors.Actor

abstract class ObservableState(val observers: List[Actor]) {
    def copy(observers: List[Actor]): ObservableState
}

abstract class Observable[T <: ObservableState] extends StateMachine[T] {

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

    private case class Observe(actor: Actor)
    private case class Ignore(actor: Actor)
    private case class OK()

    protected
    def transitions: PartialFunction[(Any,T),T]  = {
        case (Observe(caller),state) => {
            sender ! OK()
            state.copy(observers = caller :: state.observers).asInstanceOf[T]
        }
        case (Ignore(caller),state) => {
            sender ! OK()
            state.copy(observers = state.observers diff List(caller)).asInstanceOf[T]
        }
    }
}
