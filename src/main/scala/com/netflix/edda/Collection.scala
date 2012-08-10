package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import scala.util.matching.Regex

case class CollectionState(records: List[Record] = List[Record](), crawled: List[Record] = List[Record]())

object Collection extends StateMachine.LocalState[CollectionState] {
    
    case class Delta(records: List[Record], changed: List[Record], added: List[Record], removed: List[Record])
    
    // message sent to observers
    case class DeltaResult(delta: Delta) extends StateMachine.Message

    // internal messages
    private case class Load() extends StateMachine.Message
    private case class Query(query: Map[String,Any]) extends StateMachine.Message
    private case class QueryResult(records: List[Record]) extends StateMachine.Message
}

abstract class Collection(crawler: Crawler, elector: Elector) extends Observable {
    import Collection._

    def query(queryMap: Map[String,Any]): List[Record] = {
        this !? Query(queryMap) match {
            case QueryResult(results) => results
        }
    }
    
    protected
    def doQuery(queryMap: Map[String,Any], state: StateMachine.State): List[Record] = {
        // generate function
        localState(state).records.filter( record => matcher.doesMatch(queryMap, record.toMap ) )
    }

    protected
    def matcher: Matcher = BasicRecordMatcher
    
    protected
    def load(state: StateMachine.State)
    
    protected
    def update(d: Delta, state: StateMachine.State)

    protected
    def delta(newRecords: List[Record], state: StateMachine.State): Delta = {
        val oldRecords = localState(state).records
        val oldMap = oldRecords.map( rec => rec.id -> rec ).toMap
        val newMap = newRecords.map( rec => rec.id -> rec ).toMap
        
        val removedMap = oldMap.filterNot( pair => newMap.contains(pair._1) )
        val addedMap   = newMap.filterNot( pair => oldMap.contains(pair._1) )

        val changes = newMap.filterNot( pair => {
            removedMap.contains(pair._1) || addedMap.contains(pair._1) || newMap(pair._1).sameData(oldMap(pair._1))
        })

        // need to reset mtime,stime,ctime crawled records to match what we have in memory
        val fixedRecords = newRecords.collect {
            case rec: Record if oldMap.contains(rec.id) => 
                oldMap(rec.id).copy(data=rec.data)
            case rec: Record => rec
        }

        // convert newRecords with oldRecords
        Delta(fixedRecords, changed=changes.values.toList, added=addedMap.values.toList, removed=removedMap.values.toList)
    }

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
            // only propagate if newRecords are not the same as the last crawled result
            if( newRecords ne localState(state).crawled ) {
                Actor.actor { 
                    val d: Delta = delta(newRecords, state)
                    Observable.localState(state).observers.foreach( _ ! DeltaResult(d) )
                }
                setLocalState(state, localState(state).copy(crawled=newRecords))
            }
            else state
        }
        case (DeltaResult(d),state) => {
            // only propagate if the delta records are not the same as the current cached records
            if( d.records ne localState(state).records ) {
                Actor.actor {
                    update(d,state)
                }
                setLocalState(state, localState(state).copy(records=d.records))
            }
            else state
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}
