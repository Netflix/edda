package com.netflix.edda

import scala.actors.Actor

abstract class StateMachine[T] extends Actor {
    start

    protected
    def init(): T

    protected
    def transitions: PartialFunction[(Any,T), T]

    final 
    def act() {
        var state = init
        loop {
            react {
                case message =>  {
                    if( ! transitions.isDefinedAt(message,state) ) {
                        throw new java.lang.UnsupportedOperationException("Unknown Message " + message)
                    }
                    state = transitions(message, state)
                }
            }
        }
    }
}
