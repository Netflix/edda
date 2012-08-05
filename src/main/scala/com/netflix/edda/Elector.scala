package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

case class ElectorState(val isLeader: Boolean, override val observers: List[Actor]) extends ObservableState(observers) {
    override
    def copy(observers: List[Actor]): ElectorState  = ElectorState(isLeader, observers)
    def copy(isLeader: Boolean = isLeader, observers: List[Actor] = observers) = ElectorState(isLeader, observers)
}

object Elector {
    // Message sent to observers
    case class ElectionResult(result: Boolean)
}

abstract class Elector(pollCycle: Long = 10000) extends Observable[ElectorState] {
    def isLeader(): Boolean = {
        this !? IsLeader() match {
            case Elector.ElectionResult(result) => result
            case message => throw new java.lang.UnsupportedOperationException("Failed to determine leadership: " + message);
        }
    }

    protected 
    def runElection(): Boolean

    // private messages
    private case class RunElection()
    private case class IsLeader()
    
    override
    def init() = {
        this ! RunElection()
        electionPoller()
        ElectorState(false, List[Actor](this))
    }

    protected
    def electionPoller() = {
        val elector = this
        Actor.actor {
            Actor.loop {
                Actor.reactWithin(pollCycle) {
                    case TIMEOUT => {
                        elector ! RunElection()
                    }
                }
            }
        }
    }

    protected
    def localTransitions: PartialFunction[(Any,ElectorState),ElectorState] = {
        case (RunElection(),state) => {
            Actor.actor {
                val result = runElection()
                state.observers.foreach( _ ! Elector.ElectionResult(result) )
            }
            state
        }
        case (Elector.ElectionResult(result), state) => {
            state.copy(isLeader=result)
        }
        case (IsLeader(), state) => {
            sender ! Elector.ElectionResult(state.isLeader)
            state
        }
    }
    
    override
    def transitions = localTransitions orElse super.transitions
}

