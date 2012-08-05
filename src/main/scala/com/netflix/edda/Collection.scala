package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

case class CollectionState(val records: List[Record], override val observers: List[Actor]) extends ObservableState(observers) {
    override
    def copy(observers: List[Actor]) = CollectionState(records,observers)
    def copy(records: List[Record] = records, observers: List[Actor] = observers) = CollectionState(records,observers)
}

abstract class Collection(crawler: Crawler, elector: Elector) extends Observable[CollectionState] {

    def query(queryMap: Map[String,Any]): List[Record] = {
        this !? Query(queryMap) match {
            case QueryResult(results) => results
        }
    }
    
    protected
    def doQuery(queryMap: Map[String,Any], state: CollectionState): List[Record] 

    protected
    def delta(newRecords: List[Record], state: CollectionState)

    protected
    def load(state: CollectionState)
    
    protected
    def update(d: Delta, state: CollectionState)

    protected
    def init() = {
        refresher
        crawler.addObserver(this)
        this ! Load()
        CollectionState(List[Record](),List[Actor]())
    }

    // internal messages
    private case class Load()
    private case class Query(query: Map[String,Any])
    private case class QueryResult(records: List[Record])
    private case class Delta(changed: List[Record], added: List[Record], removed: List[Record])
    private case class DeltaResult(delta: Delta)

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

    protected
    def localTransitions: PartialFunction[(Any,CollectionState),CollectionState] = {
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
            if( newRecords ne state.records ) {
                Actor.actor { 
                    delta(newRecords, state)
                }
            }
            state.copy(records=newRecords)
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
