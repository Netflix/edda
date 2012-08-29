package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import java.util.Properties

import org.slf4j.{Logger, LoggerFactory}

case class ElectorState(isLeader: Boolean = false)

object Elector extends StateMachine.LocalState[ElectorState] {
    // Message sent to observers
    case class ElectionResult(from: Actor, result: Boolean) extends StateMachine.Message

    // internal messages
    private case class RunElection(from: Actor) extends StateMachine.Message
    private case class IsLeader(from: Actor)    extends StateMachine.Message
}

abstract class Elector( ctx: ConfigContext ) extends Observable {
    import Elector._

    def isLeader(): Boolean = {
        val self = this
        this !? IsLeader(this) match {
            case ElectionResult(`self`,result) => result
            case message => throw new java.lang.UnsupportedOperationException("Failed to determine leadership: " + message);
        }
    }

    private[this] val logger = LoggerFactory.getLogger(getClass)
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
        this ! RunElection(this)
        electionPoller()
    }

    protected
    def electionPoller() = {
        val elector = this
        Utils.NamedActor(this + " poller") {
            Actor.loop {
                Actor.reactWithin(pollCycle) {
                    case TIMEOUT => {
                        elector ! RunElection(this)
                    }
                }
            }
        }
    }

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (RunElection(from),state) => {
            Utils.NamedActor(this + " election runner") {
                val result = runElection()
                Observable.localState(state).observers.foreach( _ ! ElectionResult(this,result) )
            }
            state
        }
        case (ElectionResult(from,result), state) => {
            setLocalState(state,ElectorState(result))
        }
        case (IsLeader(from), state) => {
            sender ! ElectionResult(this,localState(state).isLeader)
            state
        }
    }
    
    override
    def transitions = localTransitions orElse super.transitions
}

