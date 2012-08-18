package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import java.util.concurrent.TimeUnit
import org.joda.time.DateTime
//import net.liftweb.common.Logger
import com.weiglewilczek.slf4s.Logger

import com.netflix.servo.monitor.Monitors

case class CollectionState(records: List[Record] = List[Record](), crawled: List[Record] = List[Record](), amLeader: Boolean = false)

object Collection extends StateMachine.LocalState[CollectionState] {
    trait Context extends ConfigContext {
        def recordMatcher: RecordMatcher
    }
    
    case class RecordUpdate(oldRecord: Record, newRecord: Record)
    case class Delta(records: List[Record], changed: List[RecordUpdate], added: List[Record], removed: List[Record]) {
        override def toString = "Delta(records=" + records.size + ", changed=" + changed.size + ", added=" + added.size + ", removed=" + removed.size + ")";
    }
    
    // message sent to observers
    case class DeltaResult(delta: Delta) extends StateMachine.Message

    // internal messages
    private case class Load() extends StateMachine.Message
    private case class Query(query: Map[String,Any], limit: Int, live: Boolean) extends StateMachine.Message
    private case class QueryResult(records: List[Record]) extends StateMachine.Message {
        override def toString = "QueryResult(records=" + records.size +")";
    }
}

case class namedActor[T](name: String)(body: => T) extends Actor {
    override def toString = name
    override def act = body
    start
}

abstract class Collection( ctx: Collection.Context ) extends Observable {
    import Collection._

    private[this] val logger = Logger(getClass)

    def query(queryMap: Map[String,Any], limit: Int=0, live: Boolean = false): List[Record] = {
        this !? Query(queryMap,limit,live) match {
            case QueryResult(results) => results
        }
    }

    def name: String
    def crawler: Crawler
    def datastore: Option[Datastore]
    def elector: Elector
    
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
        firstOf( limit, localState(state).records.filter( record => ctx.recordMatcher.doesMatch(queryMap, record.toMap ) ))
    }

    protected
    def firstOf(limit: Int, records: List[Record]): List[Record] = {
        if( limit > 0 ) records.take(limit) else records
    }

    protected
    def load(): List[Record] = {
        if( datastore.isDefined ) {
            datastore.get.load()
        } else {
            logger.warn("Datastore is not available for load()")
            Nil
        }
    }
    
    protected
    def update(d: Delta) {
        if( datastore.isDefined ) {
            datastore.get.update(d)
        } else {
            logger.warn("Datastore is not available, skipping update")
        }
    }

    protected
    def delta(newRecords: List[Record], oldRecords: List[Record]): Delta = {
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
        logger.info(this + " total: " + fixedRecords.size + " changed: " + changes.size + " added: " + addedMap.size + " removed: " + removedMap.size)
        Delta(fixedRecords, changed=changes.values.toList, added=addedMap.values.toList, removed=removedMap.values.toList)
    }

    protected override
    def initState = addInitialState(super.initState, newLocalState(CollectionState(records=load())))

    protected override
    def init() {
        Monitors.registerObject("edda.collection." + name, this);
        if( datastore.isDefined ) {
            datastore.get.init
        }
        elector.addObserver(this)
        crawler.addObserver(this)
        // listen to our own DeltaResult events
        // it is a sync call so put it in another actor
        Actor.actor {
            this.addObserver(this)
        }
        refresher
    }

    def timeLeft(lastRun: DateTime, millis: Long): Long = {
        val timeLeft = millis - (DateTime.now.getMillis - lastRun.getMillis)
        if( timeLeft < 0 ) 0 else timeLeft
    }

    protected
    def refresher {
        val refresh = ctx.config.getProperty(
            "edda.collection." + name + ".refresh",
            ctx.config.getProperty("edda.collection.refresh", "60000")
        ).toLong
        val cacheRefresh = ctx.config.getProperty(
            "edda.collection." + name + ".cache.refresh",
            ctx.config.getProperty("edda.collection.cache.refresh", "10000")
        ).toLong
        namedActor(this + " refresher") {
            elector.addObserver(Actor.self)
            var amLeader = elector.isLeader()
            var lastRun = DateTime.now
            Actor.loop {
                val timeout = if ( amLeader ) refresh else cacheRefresh
                Actor.reactWithin( timeLeft(lastRun, timeout) ) {
                    case TIMEOUT => {
                        if( amLeader ) crawler.crawl() else this ! Load()
                        lastRun = DateTime.now
                    }
                    case Elector.ElectionResult(result) => {
                        // if we just became leader, then start a crawl
                        if( !amLeader && result ) {
                            crawler.crawl()
                            lastRun = DateTime.now
                        }
                        amLeader = result
                    }
                    case message => {
                        logger.error("Invalid message " + message + " from sender " + sender)
                    }
                }
            }
        }
    }

    private[this] val loadTimer   = Monitors.newTimer("edda.collection." + name + ".load")
    private[this] val loadCounter = Monitors.newCounter("edda.collection." + name + ".load.count")
    private[this] val loadErrorCounter = Monitors.newCounter("edda.collection." + name + ".load.errors")

    private[this] val updateTimer   = Monitors.newTimer("edda.collection." + name + ".update")
    private[this] val updateCounter = Monitors.newCounter("edda.collection." + name + ".update.count")
    private[this] val updateErrorCounter = Monitors.newCounter("edda.collection." + name + ".update.errors")

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Query(queryMap,limit,live),state) => {
            val replyTo = sender
            namedActor(this + " Query processor") {
                replyTo ! QueryResult(doQuery(queryMap, limit, live, state))
            }
            state
        }
        case (Load(),state) => {
            val self: Actor = this
            namedActor(this + " Load processor") {
                val stopwatch = loadTimer.start()
                var records = try {
                    load()
                } catch {
                    case e => {
                        loadErrorCounter.increment
                        throw e
                    }
                } finally {
                    stopwatch.stop()
                }
                loadCounter.increment
                logger.info("%s Loaded %d records in %.2f sec".format(this, records.size, stopwatch.getDuration(TimeUnit.MILLISECONDS)/1000.0))
                self ! Crawler.CrawlResult( if(records == Nil ) localState(state).records else records )
            }
            state
        }
        case (Crawler.CrawlResult(newRecords),state) => {
            // only propagate if newRecords are not the same as the last crawled result
            if( newRecords ne localState(state).crawled ) {
                namedActor(this + " CrawlResult processor") {
                    val d: Delta = delta(newRecords, localState(state).records)
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
                        val stopwatch = updateTimer.start()
                        try {
                            update(d)
                            updateCounter.increment
                        } catch {
                            case e => {
                                updateErrorCounter.increment
                                throw e
                            }
                        } finally {
                            stopwatch.stop()
                        }
                        logger.info("%s Updated %d records(Changed: %d, Added: %d, Removed: %d) in %.2f sec".format(
                            this, d.records.size, d.changed.size, d.added.size, d.removed.size, stopwatch.getDuration(TimeUnit.MILLISECONDS)/1000.0
                        ))
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

    override
    def toString = "[Collection " + name + "]"

    override
    def start(): Actor = {
        logger.info("Staring "+ this);
        elector.start()
        crawler.start()
        super.start()
    }

    override
    def stop() {
        logger.info("Stoping "+ this);
        elector.stop()
        crawler.stop()
        super.stop()
    }
}
