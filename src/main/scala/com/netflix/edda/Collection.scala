package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

case class CollectionState(records: List[Record] = List[Record]())

object Collection extends StateMachine.LocalState[CollectionState] {
    type LocalState = CollectionState
    
    // internal messages
    private case class Load() extends StateMachine.Message
    private case class Query(query: Map[String,Any]) extends StateMachine.Message
    private case class QueryResult(records: List[Record]) extends StateMachine.Message
    private case class Delta(changed: List[Record], added: List[Record], removed: List[Record]) extends StateMachine.Message
    private case class DeltaResult(delta: Delta) extends StateMachine.Message
}

abstract class Collection(crawler: Crawler, elector: Elector) extends Observable {
    import Collection._

    def query(queryMap: Map[String,Any]): List[Record] = {
        this !? Query(queryMap) match {
            case QueryResult(results) => results
        }
    }
    
    protected
    def doQuery(queryMap: Map[String,Any], state: StateMachine.State): List[Record] 

    protected
    def delta(newRecords: List[Record], state: StateMachine.State)

    protected
    def load(state: StateMachine.State)
    
    protected
    def update(d: Delta, state: StateMachine.State)

    protected override
    def initState = addInitialState(super.initState, newLocalState(CollectionState()))

    protected override
    def init {
        refresher
        crawler.addObserver(this)
        this ! Load()
    }

    protected
    def refresher {
        val collection = this
        Actor.actor {
            elector.addObserver(this)
            var amLeader = elector.isLeader()
            if ( amLeader ) collection.addObserver(collection) else collection.delObserver(collection)
            Actor.loop {
                val timeout = if ( amLeader ) 60000 else 10000
                Actor.reactWithin(timeout) {
                    case TIMEOUT => {
                        if( amLeader ) crawler.crawl() else collection ! Load()
                    }
                    case Elector.ElectionResult(result) => {
                        if( amLeader && !result ) {
                            // demoted so stop observing to crawl deltas
                            collection.delObserver(collection)
                        }
                        else if( !amLeader && result ) {
                            // promoted so start observing to crawl deltas
                            collection.addObserver(collection)
                        }
                        amLeader = result
                    }
                    case message => throw new java.lang.UnsupportedOperationException("Unknown Message " + message);
                }
            }
        }
    }

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Query(queryMap),state) => {
            val replyTo = sender
            Actor.actor {
                replyTo ! QueryResult(doQuery(queryMap, state))
            }
            state
        }
        case (Load(),state) => {
            Actor.actor {
                load(state)
            }
            state
        }
        case (Crawler.CrawlResult(newRecords),state) => {
            if( newRecords ne localState(state).records ) {
                Actor.actor { 
                    delta(newRecords, state)
                }
            }
            setLocalState(state, CollectionState(newRecords))
        }
        case (DeltaResult(d),state) => {
            Actor.actor {
                update(d,state)
            }
            state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}
