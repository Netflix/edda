/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.edda

import scala.actors.Actor
import scala.actors.TIMEOUT
// import scala.actors.scheduler.ForkJoinScheduler
import scala.util.Random

import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.netflix.servo.monitor.Monitors
import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge
import java.lang

/** local state class for Collection
  *
  * @param records the current active records for the collection
  * @param crawled the results from the Crawler
  */
case class CollectionState(records: Seq[Record] = Seq[Record](), crawled: Seq[Record] = Seq[Record]())

/** companion object for Collection*/
object Collection extends StateMachine.LocalState[CollectionState] {

  /** Collections need a recordMatcher as well as the ConfigContext to handle querying the inMemory record set. */
  trait Context extends ConfigContext {
    def recordMatcher: RecordMatcher
  }

  /** class to represent a record that has changed, used for the DataStore to update records */
  case class RecordUpdate(oldRecord: Record, newRecord: Record)

  /** class to represent a complete delta between old record set and new record set (new from Crawler)
    *
    * @param records the current set of active records
    * @param changed the list of RecordUpdate for records that have changed
    * @param added   the list of new records (new records that Crawler found)
    * @param removed the list of records that are not longer active (were not returned from Crawler)
    */
  case class Delta(records: Seq[Record], changed: Seq[RecordUpdate], added: Seq[Record], removed: Seq[Record]) {
    override def toString = "Delta(records=" + records.size + ", changed=" + changed.size + ", added=" + added.size + ", removed=" + removed.size + ")"
  }

  /** Message sent to observers after a collection has been updated */
  case class DeltaResult(from: Actor, delta: Delta) extends StateMachine.Message

  /** Message to Load the record set from the DataStore */
  case class Load(from: Actor, full: Boolean = false) extends StateMachine.Message

  /** Messsage to *Synchronously* Load the record set from the DataStore */
  case class SyncLoad(from: Actor) extends StateMachine.Message

  /** Response from the SyncLoad request */
  case class OK(from: Actor) extends StateMachine.Message

}

/** general Collection logic.  It is abstract to specify the collection name,
  * responsible Crawler, and optional DataStore and the Elector to determine leadership.
  *
  * @param ctx context to get config, recordMatcher
  */
abstract class Collection(val ctx: Collection.Context) extends Queryable {

  import Collection._
  import Queryable._
  import Utils._

  val logger = LoggerFactory.getLogger(getClass)
  lazy val enabled = Utils.getProperty(ctx.config, "edda.collection", "enabled", name, "true").toBoolean

  /** name of the collection, typically the name of the corresponding crawler also.  Something like
    * test.us-east-1.aws.autoScalingGroups
    */
  def name: String

  /** the Crawler that will we will observe for Crawled records.  The Crawler will send us
    * records and we will compare them with our in-memory records to determine changes.
    */
  def crawler: Crawler

  /** the optional abstracted DataStore.  MongoDB is currently the only available DataStore
    * but more could be added.  It is optional so you can run without a datastore, although many
    * features will be limited (only current state is available, so no history queries possible)
    */
  def dataStore: Option[DataStore]

  /** The elector to determine leadership. This is typically a singleton so all Collections share
    * the same Election results, but it could be customized if we need to have multiple leaders handling
    * different Collections.
    */
  def elector: Elector


  /** allow option to skip cache usage and go straigt to datastore
   */
  lazy val liveOverride = Utils.getProperty(ctx.config, "edda.collection", "noCache", name, "false").toBoolean

  override
  lazy val queryTimeout = Utils.getProperty(ctx.config, "edda.collection", "queryTimeout", name, "60000").toLong

  // /** use separate ForkJoin scheduler for the Collection actors so one Collection doesn't end
  //   * up starving the global actor pool.
  //   */
  // lazy val fjScheduler = new ForkJoinScheduler(
  //   Utils.getProperty(ctx.config, "edda.collection", "scheduler.coreSize", name, "5").toInt,
  //   Utils.getProperty(ctx.config, "edda.collection", "scheduler.maxSize", name, "50").toInt,
  //   true,
  //   true
  // )
  // override def scheduler = fjScheduler

  /** see [[com.netflix.edda.Queryable.query()]].  Overridden to return Nil when Collection is not enabled */
  override def query(queryMap: Map[String, Any] = Map(), limit: Int = 0, live: Boolean = false, keys: Set[String] = Set(), replicaOk: Boolean = false)(events: EventHandlers = DefaultEventHandlers): Nothing = {
    if (enabled) super.query(queryMap, limit, live || liveOverride, keys, replicaOk)(events) else Actor.self.reactWithin(0) {
      case msg @ TIMEOUT => {
        logger.debug(Actor.self + " received: " + msg + " for disabled collection")
        events(Success(QueryResult(Actor.self,Seq.empty)))
      }
    }
  }

  /** see [[com.netflix.edda.Observable.addObserver()]].  Overridden to be a NoOp when Collection is not enabled */
  override def addObserver(actor: Actor)(events: EventHandlers = DefaultEventHandlers): Nothing = {
    if (enabled) super.addObserver(actor)(events) else Actor.self.reactWithin(0) { 
      case msg @ TIMEOUT => {
        logger.debug(Actor.self + " received: " + msg + " for disabled collection")
        events(Success(Observable.OK(Actor.self)))
      }
    }
  }

  /** see [[com.netflix.edda.Observable.delObserver()]].  Overridden to be a NoOp when Collection is not enabled */
  override def delObserver(actor: Actor)(events: EventHandlers = DefaultEventHandlers): Nothing = {
    if (enabled) super.delObserver(actor)(events) else Actor.self.reactWithin(0) {
      case msg @ TIMEOUT => {
        logger.debug(Actor.self + " received: " + msg + " for disabled collection")
        events(Success(Observable.OK(Actor.self)))
      }
    }
  }

  /** query datastore or in memory collection. */
  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean, state: StateMachine.State): Seq[Record] = {
    // generate function
    if (live || liveOverride) {
      if (dataStore.isDefined) {
        return dataStore.get.query(queryMap, limit, keys, replicaOk)
      } else {
        logger.warn("DataStore is not available, applying query to cached records")
      }
    }
    val recs = if (queryMap.isEmpty) {
      firstOf(limit, localState(state).records)
    } else {
      firstOf(limit, localState(state).records.filter(record => ctx.recordMatcher.doesMatch(queryMap, record.toMap)))
    }
    if( dataStore.isDefined ) {
        val mtime = dataStore.get.collectionModified
        recs.map(_.copy(mtime=mtime))
    } else recs
  }

  /** load collection from Datastore (if available) */
  protected def load(replicaOk: Boolean): Seq[Record] = {
    if (dataStore.isDefined) {
      val now = DateTime.now
      val records = dataStore.get.load(replicaOk)
      lastLoad = records match {
          case Nil => now
          case _: Seq[_] => records.maxBy( _.mtime.getMillis ).mtime
      }
      lastFullLoad = now
      records
    } else {
      logger.warn("DataStore is not available for load()")
      Seq()
    }
  }

  protected def update(d: Delta) {
    if (dataStore.isDefined) {
      dataStore.get.update(d)
    } else {
      logger.warn("DataStore is not available, skipping update")
    }
  }

  /** customize how a record change is handled.  If it returns true
    * a new document revision is created in the dataStore, if it is false
    * the original document is updated (new document revision not created)
    */
  protected def newStateTimeForChange(newRec: Record, oldRec: Record) = true

  /** calculate the difference between the records from a Crawl result and the records
    * currently in memory.
    *
    * @param newRecords records from the Crawler
    * @param oldRecords records from previous Delta result
    */
  protected def delta(newRecordsIn: Seq[Record], oldRecords: Seq[Record])(events: EventHandlers = DefaultEventHandlers): Nothing = {
    val now = DateTime.now
    val newRecords = newRecordsIn.map( rec => rec.copy(mtime = now) )

    // remove needs to be a list to allow for duplicate records (multiple record revisions
    // on the same id)
    var remove = Seq[Record]()

    // sometimes there are duplicates in oldRecords (upon first-load when we load all records
    // with null ltime) when we have a rogue writer (sometimes there are gaps between leadership
    // changes). 
    val oldSeen = scala.collection.mutable.Map[String,Record]()
    val oldMap = oldRecords.filter(r => {
      val in = oldSeen.contains(r.id)
      if( in ) {
          val lastSeen = oldSeen(r.id).mtime
          remove +:= r.copy(mtime=lastSeen,ltime=lastSeen)
      } else {
          oldSeen += (r.id -> r)
      }
      !in
    }).map(rec => rec.id -> rec).toMap
    val newMap = newRecords.map(rec => rec.id -> rec).toMap

    remove ++= oldMap.filterNot(pair => newMap.contains(pair._1)).map(
      pair => pair._2.copy(mtime = now, ltime = now))

    val addedMap = newMap.filterNot(pair => oldMap.contains(pair._1))

    val changes = newMap.filter(pair => {
      oldMap.contains(pair._1) && !pair._2.sameData(oldMap(pair._1))
    }).map(
      pair => {
        val oldRec = oldMap(pair._1)
        val newRec = pair._2
        if (newStateTimeForChange(newRec, oldRec)) {
          pair._1 -> Collection.RecordUpdate(oldRec.copy(mtime = now, ltime = now), newRec)
        } else {
          pair._1 -> Collection.RecordUpdate(oldRec.copy(mtime = now, ltime = now), newRec.copy(stime = oldRec.stime))
        }
      }
    )

    // need to reset stime, ctime, tags for crawled records to match what we have in memory
    val fixedRecords = newRecords.collect {
      case rec: Record if changes.contains(rec.id) => {
        val newRec = changes(rec.id).newRecord
        oldMap(rec.id).copy(data = rec.data, mtime = newRec.mtime, stime = newRec.stime)
      }
      case rec: Record if oldMap.contains(rec.id) =>
        oldMap(rec.id).copy(data = rec.data, mtime = rec.mtime)
      case rec: Record => rec
    }

    logger.info(this + " total: " + fixedRecords.size + " changed: " + changes.size + " added: " + addedMap.size + " removed: " + remove.size)
    Actor.self.reactWithin(0) {
      case TIMEOUT => {
        events(Success(Delta(fixedRecords, changed = changes.values.toSeq, added = addedMap.values.toSeq, removed = remove)))
      }
    }
  }

  /** setup CollectionState, initialize the records to be loaded from the DataStore before the Actor starts accepting message */
  protected override def initState = addInitialState(super.initState, newLocalState(CollectionState(records = load(replicaOk = true))))

  /** initialize servo metrics for Collection.  Delay start based on random jitter to prevent DataStore from being
    * overloaded by all Collection loading all at once.
    */
  protected override def init() {
    Monitors.registerObject("edda.collection." + name, this)
    Utils.NamedActor(this + " init") {
      // create routine to run after the jitter timeout
      // or to run immediately if jitter is disabled
      def postJitter: Nothing = {
        if (dataStore.isDefined) {
          dataStore.get.init()
        }
        
        // routine to run on success of crawler addObserver call
        // or to run immediately if crawler is disabled
        def postObserver = {
          refresher()
          // super.init will cause normal event processing to start on this
          // collection actor, so the next addObserver should procceed
          super.init()
          // listen to our own DeltaResult events
          def retry: Nothing = {
            this.addObserver(this) {
              case Success(msg) => 
              case Failure(msg) => {
                logger.error(Actor.self + " failed to add observer " + this + " to " + this + " with error: " + msg + ", retrying")
                retry
              }
            }
          }
          retry
        }
        
        if( Option(crawler).isDefined ) {
          crawler.addObserver(this) {
            case Success(msg) => postObserver
            case Failure(msg) => {
              logger.error(Actor.self + " failed to add observer " + this + " to " + crawler + " with error: " + msg + ", retrying")
              postJitter
            }
          }
        }
        else postObserver
      }
      
      if (Utils.getProperty(ctx.config, "edda.collection", "jitter.enabled", name, "true").toBoolean) {
        val cacheRefresh = Utils.getProperty(ctx.config, "edda.collection", "cache.refresh", name, "10000").toLong
        // adding in random jitter on start so we dont crush the datastore immediately if multiple
        // systems are coming up at the same time
        val rand = new Random
        val jitter = (cacheRefresh * rand.nextDouble).toLong
        logger.info(this + " start delayed by " + jitter + "ms")
        Actor.self.reactWithin(jitter) {
          case msg @ TIMEOUT => {
            logger.debug(Actor.self + " received: " + msg + " for jitter timeout")
            postJitter
          }
        }
      }
      else postJitter
    }
  }
  
  /** helper routine to calculate timeLeft before a Crawl request shoudl be made */
  def timeLeft(lastRun: DateTime, millis: Long): Long = {
    val timeLeft = millis - (DateTime.now.getMillis - lastRun.getMillis)
    if (timeLeft < 0) 0 else timeLeft
  }

  /** responsible for asking the Crawler to crawl if we are the leader, or if we are not the leader
    * responsible for reloading the in-memory cache.  Also responsible for receiving election
    * results to take over leaderhip when necessary.
    */
  protected def refresher() {
    if (Option(crawler) == None || Option(elector) == None) return
    val refresh = Utils.getProperty(ctx.config, "edda.collection", "refresh", name, "60000").toLong
    val cacheRefresh = Utils.getProperty(ctx.config, "edda.collection", "cache.refresh", name, "10000").toLong
    val cacheFullRefresh = Utils.getProperty(ctx.config, "edda.collection", "cache.full.refresh", name, "1800000").toLong
    NamedActor(this + " refresher") {
      elector.addObserver(Actor.self) {
        case Failure(msg) => {
          logger.error(Actor.self + " failed to addObserver: " + msg)
          refresher
        }
        case Success(msg) => {
          var amLeader = false
          // crawl immediately the first time
          if (amLeader) crawler.crawl()
          
          var lastRun = DateTime.now
          Actor.self.loop {
            val timeout = if (amLeader) refresh else cacheRefresh
            Actor.self.reactWithin(timeLeft(lastRun, timeout)) {
              case msg @ TIMEOUT => {
                logger.debug(Actor.self + " received: " + msg)
                val full = if( timeLeft(lastFullLoad, cacheFullRefresh) > 0 ) false else true
                if (amLeader) { 
                  crawler.crawl()
                }
                else {
                  val msg = Load(Actor.self,full)
                  logger.debug(Actor.self + " sending: " + msg + " -> " + this)
                  this ! msg
                }
                lastRun = DateTime.now
              }
              case msg @ Elector.ElectionResult(from, result) => {
                logger.debug(Actor.self + " received: " + msg + " from " + sender)
                // if we just became leader, then start a crawl
                if (!amLeader && result) {
                  val msg = SyncLoad(Actor.self)
                  logger.debug(Actor.self + " sending: " + msg + " -> " + this)
                  this ! msg
                  Actor.self.reactWithin(300000) {
                    case msg @ OK(frm) => {
                      crawler.crawl()
                      lastRun = DateTime.now
                      amLeader = result
                    }
                    case msg @ TIMEOUT => {
                      logger.error(this + " failed to reload data in 5m as we became leader")
                      throw new java.lang.RuntimeException("TIMEOUT: " + this + " Failed to reload data in 5m as we became leader")
                    }
                  }
                }
                else amLeader = result
              }
            }
          }
        }
      }
    }.addExceptionHandler({
      case e: Exception => logger.error(this + " failed to refresh", e)
    })
  }

  // basic servo metrics
  private[this] val loadTimer = Monitors.newTimer("load")
  private[this] val loadCounter = Monitors.newCounter("load.count")
  private[this] val loadErrorCounter = Monitors.newCounter("load.errors")

  private[this] val updateTimer = Monitors.newTimer("update")
  private[this] val updateCounter = Monitors.newCounter("update.count")
  private[this] val updateErrorCounter = Monitors.newCounter("update.errors")

  private[this] var lastCrawl = DateTime.now
  private[this] val crawlGauge: BasicGauge[lang.Long] = new BasicGauge[java.lang.Long](
    MonitorConfig.builder("lastCrawl").build(),
    new Callable[java.lang.Long] {
      def call() = {
        if (elector.isLeader) {
          DateTime.now.getMillis - lastCrawl.getMillis
        } else 0
      }
    })

  private[this] var lastFullLoad: DateTime = new DateTime(0)
  private[this] var lastLoad: DateTime = new DateTime(0)

  // eliminate used-only-once warnings from IntelliJ
  if(false) crawlGauge

  /** load records from DataStore and update monitoring metrics */
  private def doLoad(replicaOk: Boolean): Seq[Record] = {
    val stopwatch = loadTimer.start()
    val records = try {
      load(replicaOk)
    } catch {
      case e: Exception => {
        loadErrorCounter.increment()
        throw e
      }
    } finally {
      stopwatch.stop()
    }
    loadCounter.increment()
    logger.info("{} Loaded {} records in {} sec", toObjects(
      this, records.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000.0 -> "%.2f"))
    records
  }

  /** handle Collection Messages */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (SyncLoad(from), state) => {
      // SyncLoad allows us to make sure we have a current cache in memory of "live" records
      // before we take over as "Leader" and start writing to the DataStore
      val replyTo = sender
      NamedActor(this + " SyncLoad processor") {
        val records = doLoad(replicaOk = false)
        val msg = Crawler.CrawlResult(this, if (records.size == 0) localState(state).records else records)
        logger.debug(this + " sending: " + msg + " -> " + this)
        this ! msg
        val msg2 = OK(this)
        logger.debug(this + " sending: " + msg2 + " -> " + replyTo)
        replyTo ! msg2
      }
      state
    }
    case (Load(from, full), state) => {
      NamedActor(this + " Load processor") {
          val stopwatch = loadTimer.start()
          val records = try {
              if( full ) {
                  logger.info(this + " doing full reload of collection");
                  doLoad(replicaOk = true)
              }
              else {
                  val recs = doQuery(Map("mtime" -> Map("$gte" -> lastLoad)), limit = 0, live = true, keys=Set(), replicaOk = true, state)
                  if( recs.size == 0 ) {
                      localState(state).records
                  } else {
                      lastLoad = recs.maxBy( _.mtime.getMillis ).mtime
                      val seen = scala.collection.mutable.Set[String]()
                      val uniqRecs = recs.filter(r => {
                          val in = seen.contains(r.id)
                          if( !in ) seen += r.id
                          !in
                      })
                      
                      val addRecs = uniqRecs.filter( rec => rec.ltime == null )
                      val delRecs = uniqRecs.filter( rec => rec.ltime != null )
                      
                      val oldMap = localState(state).records.map(rec => rec.id -> rec).toMap
                      val addMap = addRecs.map( rec => rec.id -> rec).toMap
                      ((oldMap ++ addMap) -- delRecs.map(_.id)).values.toSeq.sortWith((a, b) => a.stime.isAfter(b.stime))
                  }
              }
          } catch {
              case e: Exception => {
                  loadErrorCounter.increment()
                  throw e
              }
          } finally {
                  stopwatch.stop()
          }
          loadCounter.increment()
          logger.info("{} Loaded {} records in {} sec", toObjects(
              this, records.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000.0 -> "%.2f"))

          val msg = Crawler.CrawlResult(this, if (records.size == 0) localState(state).records else records)
          logger.debug(this + " sending: " + msg + " -> " + this)
          this ! msg
      }
      state
    }
    case (Crawler.CrawlResult(from, newRecords), state) => {
      // only propagate if newRecords are not the same as the last crawled result
      lastCrawl = DateTime.now
      if (newRecords ne localState(state).crawled) {
        NamedActor(this + " CrawlResult processor") {
          def processDelta(d: Delta) = {
            lazy val path = name.replace('.', '/')
            d.added.foreach(
              rec => {
                logger.info("Added {}/{};_pp;_at={}", toObjects(path, rec.id, rec.stime.getMillis))
              })
            d.removed.foreach(
              rec => {
                logger.info("Removing {}/{};_pp;_at={}", toObjects(path, rec.id, rec.stime.getMillis))
              })
            d.changed.foreach(
              update => {
                lazy val diff: String = Utils.diffRecords(Array(update.newRecord, update.oldRecord), Some(1), path)
                logger.info("\n{}", diff)
              })
            
            val msg = DeltaResult(this, d)
            Observable.localState(state).observers.foreach(o => {
              logger.debug(this + " sending: " + msg + " -> " + o)
              o ! msg
            })
          }

          if (from == this) {
            // this is from a Load so no need to calculate Delta
            processDelta(Delta(newRecords, Seq(), Seq(), Seq()))
          } else {
            delta(newRecords, localState(state).records) { 
              case Failure(error) => {
                logger.error(this + " delta failed: " + error)
                throw new java.lang.RuntimeException(this + " delta failed: " + error)
              }
              case Success(delta: Delta) => processDelta(delta)
            }
          }
        }
        setLocalState(state, localState(state).copy(crawled = newRecords))
      } else state
    }
    case (DeltaResult(from, d), state) => {
      // only propagate if the delta records are not the same as the current cached records
      if ((d.records ne localState(state).records)) {
        if (elector.isLeader) {
          Actor.actor {
            val stopwatch = updateTimer.start()
            try {
              update(d)
              updateCounter.increment()
            } catch {
              case e: Exception => {
                updateErrorCounter.increment()
                throw e
              }
            } finally {
              stopwatch.stop()
            }
            logger.info("{} Updated {} records(Changed: {}, Added: {}, Removed: {}) in {} sec", toObjects(
              this, d.records.size, d.changed.size, d.added.size, d.removed.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000.0 -> "%.2f"))
          }
        }
        setLocalState(state, localState(state).copy(records = d.records))
      } else state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions

  override def toString = "[Collection " + name + "]"

  /** if collection is enabled start elector, start crawler first */
  override def start(): Actor = {
    if (enabled) {
      logger.info("Starting " + this)
      Option(elector).foreach(_.start())
      Option(crawler).foreach(_.start())
      super.start()
    } else {
      logger.info("Collection " + name + " is disabled, not starting")
      this
    }
  }

  /** stop elector, crawler and shutdown ForkJoin special scheduler */
  override def stop() {
    logger.info("Stopping " + this)
    Option(elector).foreach(_.stop())
    Option(crawler).foreach(_.stop())
    // fjScheduler.shutdown()
    super.stop()
  }
}

/** for setting the name on Collections when we are tracking many accounts with same root name.  Used with MergedCollection
  * so we could have test.us-east-1.aws.autoScalingGroups and test.us-west-1.aws.autoScalingGroups independent collections
  * but then have a MergedCollection called "aws.autoScalingGroups" that will dispatch queries to both collections.
  * @param rootName base name of Collection (ie aws.autoScalingGroups)
  * @param accountName name of account (ie test.us-east-1)
  * @param ctx the collection context for config and recordMatcher
  */
abstract class RootCollection(val rootName: String, accountName: String, ctx: Collection.Context) extends Collection(ctx) {
  val name = accountName match {
    case "" => rootName
    case x: String => x + "." + rootName
  }
}
