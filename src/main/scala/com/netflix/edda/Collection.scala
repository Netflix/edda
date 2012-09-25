package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT

import java.util.concurrent.TimeUnit
import org.joda.time.DateTime
import org.slf4j.{Logger, LoggerFactory}

import com.netflix.servo.monitor.Monitors

case class CollectionState(records: Seq[Record] = Seq[Record](), crawled: Seq[Record] = Seq[Record](), amLeader: Boolean = false)

object Collection extends StateMachine.LocalState[CollectionState] {
    trait Context extends ConfigContext {
        def recordMatcher: RecordMatcher
    }
    
    case class RecordUpdate(oldRecord: Record, newRecord: Record)
    case class Delta(records: Seq[Record], changed: Seq[RecordUpdate], added: Seq[Record], removed: Seq[Record]) {
        override def toString = "Delta(records=" + records.size + ", changed=" + changed.size + ", added=" + added.size + ", removed=" + removed.size + ")";
    }
    
    // message sent to observers
    case class DeltaResult(from: Actor, delta: Delta) extends StateMachine.Message

    // internal messages
    case class Load(from: Actor) extends StateMachine.Message
}

abstract class Collection( val ctx: Collection.Context ) extends Queryable {
    import Collection._
    import Utils._

    val logger = LoggerFactory.getLogger(getClass)
    lazy val enabled = Utils.getProperty(ctx.config,"edda.collection", "enabled", name, "true").toBoolean

    def name: String
    def crawler: Crawler
    def datastore: Option[Datastore]
    def elector: Elector
    
    override
    def query(queryMap: Map[String,Any] = Map(), limit: Int=0, live: Boolean = false, keys: Set[String] = Set()): Seq[Record] = {
        if(enabled) super.query(queryMap,limit,live,keys) else Seq.empty
    }

    override def addObserver(actor: Actor) = if(enabled) super.addObserver(actor)
    override def delObserver(actor: Actor) = if(enabled) super.delObserver(actor)

    protected
    def doQuery(queryMap: Map[String,Any], limit: Int, live: Boolean, keys: Set[String], state: StateMachine.State): Seq[Record] = {
        // generate function
        if( live ) {
            if( datastore.isDefined ) {
                return datastore.get.query(queryMap, limit, keys)
            } else {
                logger.warn("Datastore is not available, applying query to cached records")
            }
        }
        if(queryMap.isEmpty) {
            firstOf( limit, localState(state).records)
        } else {
            firstOf( limit, localState(state).records.filter( record => ctx.recordMatcher.doesMatch(queryMap, record.toMap ) ))
        }
    }

    protected
    def load(): Seq[Record] = {
        if( datastore.isDefined ) {
            datastore.get.load()
        } else {
            logger.warn("Datastore is not available for load()")
            Seq()
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
    def delta(newRecords: Seq[Record], oldRecords: Seq[Record]): Delta = {
        val oldMap = oldRecords.map( rec => rec.id -> rec ).toMap
        val newMap = newRecords.map( rec => rec.id -> rec ).toMap
        
        val now=DateTime.now

        val removedMap = oldMap.filterNot( pair => newMap.contains(pair._1) ).map(
            pair => pair._1 -> pair._2.copy(mtime=now,ltime=now)
        )
        val addedMap   = newMap.filterNot( pair => oldMap.contains(pair._1) )

        val changes = newMap.filter( pair => {
            oldMap.contains(pair._1) && newMap.contains(pair._1) && !newMap(pair._1).sameData(oldMap(pair._1))
        }).map( pair => pair._1 -> RecordUpdate(oldMap(pair._1).copy(mtime=now,ltime=now), pair._2) )

        // need to reset stime,ctime,tags for crawled records to match what we have in memory
        val fixedRecords = newRecords.collect {
            case rec: Record if changes.contains(rec.id) =>
                oldMap(rec.id).copy(data=rec.data, mtime=rec.mtime, stime=rec.stime)
            case rec: Record if oldMap.contains(rec.id) => 
                oldMap(rec.id).copy(data=rec.data, mtime=rec.mtime)
            case rec: Record => rec
        }

        logger.info(this + " total: " + fixedRecords.size + " changed: " + changes.size + " added: " + addedMap.size + " removed: " + removedMap.size)
        Delta(fixedRecords, changed=changes.values.toSeq, added=addedMap.values.toSeq, removed=removedMap.values.toSeq)
    }

    protected override
    def initState = addInitialState(super.initState, newLocalState(CollectionState(records=load())))

    protected override
    def init() {
        Monitors.registerObject("edda.collection." + name, this);
        if( datastore.isDefined ) {
            datastore.get.init
        }
        Option(elector).foreach( _.addObserver(this) )
        Option(crawler).foreach( _.addObserver(this) )
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
        if( Option(crawler) == None || Option(elector) == None ) return
        val refresh = Utils.getProperty(ctx.config, "edda.collection", "refresh", name, "60000").toLong
        val cacheRefresh = Utils.getProperty(ctx.config, "edda.collection", "cache.refresh", name, "10000").toLong
        NamedActor(this + " refresher") {
            elector.addObserver(Actor.self)
            var amLeader = elector.isLeader()
            var lastRun = DateTime.now
            Actor.loop {
                val timeout = if ( amLeader ) refresh else cacheRefresh
                Actor.reactWithin( timeLeft(lastRun, timeout) ) {
                    case TIMEOUT => {
                        if( amLeader ) crawler.crawl() else this ! Load(this)
                        lastRun = DateTime.now
                    }
                    case Elector.ElectionResult(from, result) => {
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
        case (Load(from),state) => {
            val self: Actor = this
            NamedActor(this + " Load processor") {
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
                logger.info("{} Loaded {} records in {} sec", toObjects(
                    this, records.size, stopwatch.getDuration(TimeUnit.MILLISECONDS)/1000.0 -> "%.2f"
                ))
                self ! Crawler.CrawlResult(this, if(records.size == 0) localState(state).records else records )
            }
            state
        }
        case (Crawler.CrawlResult(from, newRecords),state) => {
            // only propagate if newRecords are not the same as the last crawled result
            if( newRecords ne localState(state).crawled ) {
                NamedActor(this + " CrawlResult processor") {
                    val d: Delta = 
                        if( from == this ) {
                            // this is from a Load so no need to calculate Delta
                            Delta(newRecords,Seq(),Seq(),Seq())
                        }
                        else {
                            delta(newRecords, localState(state).records)
                        }
                    
                    lazy val path = name.replace('.','/')
                    d.added.foreach(
                        rec => {
                            logger.info("Added {}/{};_pp;_at={}", toObjects(path, rec.id, rec.stime.getMillis))
                        }
                    )
                    d.removed.foreach(
                        rec => {
                            logger.info("Removing {}/{};_pp;_at={}", toObjects(path, rec.id, rec.stime.getMillis))
                        }
                    )
                    d.changed.foreach( 
                        update => {
                            lazy val diff: String = Utils.diffRecords(Array(update.newRecord, update.oldRecord), Some(1), path)
                            logger.info("\n{}", diff)
                        }
                    )
                    
                    Observable.localState(state).observers.foreach( _ ! DeltaResult(this, d) )
                }
                setLocalState(state, localState(state).copy(crawled=newRecords))
            }
            else state
        }
        case (DeltaResult(from, d),state) => {
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
                        logger.info("{} Updated {} records(Changed: {}, Added: {}, Removed: {}) in {} sec", toObjects(
                            this, d.records.size, d.changed.size, d.added.size, d.removed.size, stopwatch.getDuration(TimeUnit.MILLISECONDS)/1000.0 -> "%.2f"
                        ))
                    }
                }
                setLocalState(state, localState(state).copy(records=d.records))
            }
            else state
        }
        case (Elector.ElectionResult(from, result),state) => {
            setLocalState(state, localState(state).copy(amLeader=result))
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions

    override
    def toString = "[Collection " + name + "]"

    override
    def start(): Actor = {
        if( enabled ) { 
            logger.info("Staring "+ this);
            Option(elector).foreach( _.start )
            Option(crawler).foreach( _.start )
            super.start()
        } else {
            logger.info("Collection " + name + " is disabled, not starting")
            this
        }
    }

    override
    def stop() {
        logger.info("Stoping "+ this);
        Option(elector).foreach( _.stop )
        Option(crawler).foreach( _.stop )
        super.stop()
    }
}

// for having many accounts with same root name but for various accounts
abstract class RootCollection(val rootName: String, accountName: String, ctx: Collection.Context) extends Collection(ctx) {
    val name = accountName match {
        case "" => rootName
        case x: String => x + "." + rootName
    }
}
