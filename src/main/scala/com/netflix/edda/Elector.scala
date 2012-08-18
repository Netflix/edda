package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import java.util.Properties

import com.weiglewilczek.slf4s.Logger

case class ElectorState(isLeader: Boolean = false)

object Elector extends StateMachine.LocalState[ElectorState] {
    // Message sent to observers
    case class ElectionResult(result: Boolean) extends StateMachine.Message

    // internal messages
    private case class RunElection() extends StateMachine.Message
    private case class IsLeader()    extends StateMachine.Message
}

abstract class Elector( ctx: ConfigContext ) extends Observable {
    import Elector._

    def isLeader(): Boolean = {
        this !? IsLeader() match {
            case ElectionResult(result) => result
            case message => throw new java.lang.UnsupportedOperationException("Failed to determine leadership: " + message);
        }
    }

    private[this] val logger = Logger(getClass)
    val pollCycle = ctx.config.getProperty("edda.elector.refresh", "10000").toInt

    protected 
    def runElection(): Boolean

    protected override
    def initState = addInitialState(super.initState, newLocalState(ElectorState()))

    protected override
    def init = {
        // it is a sync call so put it in another thread
        Actor.actor {
            this.addObserver(this)
        }
        this ! RunElection()
        electionPoller()
    }

    protected
    def electionPoller() = {
        val elector = this
        namedActor(this + " poller") {
            Actor.loop {
                Actor.reactWithin(pollCycle) {
                    case TIMEOUT => {
                        elector ! RunElection()
                    }
                }
            }
        }
    }

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (RunElection(),state) => {
            namedActor(this + " election runner") {
                val result = runElection()
                Observable.localState(state).observers.foreach( _ ! ElectionResult(result) )
            }
            state
        }
        case (ElectionResult(result), state) => {
            setLocalState(state,ElectorState(result))
        }
        case (IsLeader(), state) => {
            sender ! ElectionResult(localState(state).isLeader)
            state
        }
    }
    
    override
    def transitions = localTransitions orElse super.transitions
}

