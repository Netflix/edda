package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import java.util.Properties

case class CollectionState(records: List[Record] = List[Record](), crawled: List[Record] = List[Record](), amLeader: Boolean = false)

object Collection extends StateMachine.LocalState[CollectionState] {
    
    case class RecordUpdate(oldRecord: Record, newRecord: Record)
    case class Delta(records: List[Record], changed: List[RecordUpdate], added: List[Record], removed: List[Record])
    
    // message sent to observers
    case class DeltaResult(delta: Delta) extends StateMachine.Message

    // internal messages
    private case class Load() extends StateMachine.Message
    private case class Query(query: Map[String,Any], limit: Int, live: Boolean) extends StateMachine.Message
    private case class QueryResult(records: List[Record]) extends StateMachine.Message
}

trait Collection extends Observable with NamedComponent with ConfigurationComponent with CrawlerComponent with DatastoreComponent with ElectorComponent with RecordMatcherComponent {
    import Collection._

    private[this] val logger = LoggerFactory.getLogger(getClass)

    def query(queryMap: Map[String,Any], limit: Int=0, live: Boolean = false): List[Record] = {
        this !? Query(queryMap,limit,live) match {
            case QueryResult(results) => results
        }
    }
    
    protected
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, state: StateMachine.State): List[Record] = {
        // generate function
        if( live ) {
            if( datastore.isDefined ) {
                return datastore.get.query(queryMap, limit)
            } else {
                logger.warn("Datastore is not available, applying query to cached records")
            }
        }
        firstOf( limit, localState(state).records.filter( record => recordMatcher.doesMatch(queryMap, record.toMap ) ))
    }

    protected
    def firstOf(limit: Int, records: List[Record]): List[Record] = {
        if( limit > 0 ) records.take(limit) else records
    }

    protected
    def load(state: StateMachine.State): List[Record] = {
        if( datastore.isDefined ) {
            datastore.get.load()
        } else {
            logger.warn("Datastore is not available, returning cached records from load()")
            localState(state).records
        }
    }
    
    protected
    def update(d: Delta, state: StateMachine.State) {
        if( datastore.isDefined ) {
            datastore.get.update(d)
        } else {
            logger.warn("Datastore is not available, skipping update")
        }
    }

    protected
    def delta(newRecords: List[Record], state: StateMachine.State): Delta = {
        val oldRecords = localState(state).records
        val oldMap = oldRecords.map( rec => rec.id -> rec ).toMap
        val newMap = newRecords.map( rec => rec.id -> rec ).toMap
        
        val now=DateTime.now

        val removedMap = oldMap.filterNot( pair => newMap.contains(pair._1) ).map(
            pair => pair._1 -> pair._2.copy(mtime=now,ltime=now)
        )
        val addedMap   = newMap.filterNot( pair => oldMap.contains(pair._1) )

        val changes = newMap.filterNot( pair => {
            removedMap.contains(pair._1) || addedMap.contains(pair._1) || newMap(pair._1).sameData(oldMap(pair._1))
        }).map( pair => pair._1 -> RecordUpdate(oldMap(pair._1).copy(mtime=now,ltime=now), pair._2) )

        // need to reset stime,ctime,tags for crawled records to match what we have in memory
        val fixedRecords = newRecords.collect {
            case rec: Record if changes.contains(rec.id) =>
                oldMap(rec.id).copy(data=rec.data, mtime=now, stime=now)
            case rec: Record if oldMap.contains(rec.id) => 
                oldMap(rec.id).copy(data=rec.data, mtime=now)
            case rec: Record => rec
        }

        // convert newRecords with oldRecords
        Delta(fixedRecords, changed=changes.values.toList, added=addedMap.values.toList, removed=removedMap.values.toList)
    }

    protected override
    def initState = addInitialState(super.initState, newLocalState(CollectionState()))

    protected override
    def init() {
        if( datastore.isDefined ) {
            datastore.get.init
        }
        elector.addObserver(this)
        crawler.addObserver(this)
        // listen to our own DeltaResult events
        this.addObserver(this)
        this ! Load()
        refresher
    }

    protected
    def refresher {
        val collection = this
        val refresh = config.getProperty(
            "edda.collection." + name + ".refresh",
            config.getProperty("edda.collection.refresh", "60000")
        ).toInt
        val cacheRefresh = config.getProperty(
            "edda.collection." + name + ".cache.refresh",
            config.getProperty("edda.collection.cache.refresh", "10000")
        ).toInt
        
        Actor.actor {
            var amLeader = elector.isLeader()
            elector.addObserver(this)
            Actor.loop {
                val timeout = if ( amLeader ) refresh else cacheRefresh
                Actor.reactWithin(timeout) {
                    case TIMEOUT => {
                        if( amLeader ) crawler.crawl() else collection ! Load()
                    }
                    case Elector.ElectionResult(result) => {
                        amLeader = result
                    }
                    case message => {
                        logger.error("Invalid message " + message + " from sender " + sender)
                    }
                }
            }
        }
    }

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Query(queryMap,limit,live),state) => {
            val replyTo = sender
            Actor.actor {
                replyTo ! QueryResult(doQuery(queryMap, limit, live, state))
            }
            state
        }
        case (Load(),state) => {
            val self: Actor = this
            Actor.actor {
                self ! Crawler.CrawlResult(load(state))
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
                if( localState(state).amLeader ) {
                    Actor.actor {
                        update(d,state)
                    }
                }
                setLocalState(state, localState(state).copy(records=d.records))
            }
            else state
        }
        case (Elector.ElectionResult(result),state) => {
            setLocalState(state, localState(state).copy(amLeader=result))
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}
