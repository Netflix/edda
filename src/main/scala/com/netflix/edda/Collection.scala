/**
 * Copyright 2012 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
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

import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable
import org.joda.time.DateTime
import org.slf4j.LoggerFactory

import com.netflix.servo.monitor.Monitors
import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge
import java.lang

case class CollectionState(records: Seq[Record] = Seq[Record](), crawled: Seq[Record] = Seq[Record](), amLeader: Boolean = false)

object Collection extends StateMachine.LocalState[CollectionState] {
  trait Context extends ConfigContext {
    def recordMatcher: RecordMatcher
  }

  case class RecordUpdate(oldRecord: Record, newRecord: Record)
  case class Delta(records: Seq[Record], changed: Seq[RecordUpdate], added: Seq[Record], removed: Seq[Record]) {
    override def toString = "Delta(records=" + records.size + ", changed=" + changed.size + ", added=" + added.size + ", removed=" + removed.size + ")"
  }

  // message sent to observers
  case class DeltaResult(from: Actor, delta: Delta) extends StateMachine.Message

  // internal messages
  case class Load(from: Actor) extends StateMachine.Message
  case class SyncLoad(from: Actor) extends StateMachine.Message
  case class OK(from: Actor) extends StateMachine.Message
}

abstract class Collection(val ctx: Collection.Context) extends Queryable {
  import Collection._
  import Utils._

  val logger = LoggerFactory.getLogger(getClass)
  lazy val enabled = Utils.getProperty(ctx.config, "edda.collection", "enabled", name, "true").toBoolean

  def name: String
  def crawler: Crawler
  def dataStore: Option[DataStore]
  def elector: Elector

  override def query(queryMap: Map[String, Any] = Map(), limit: Int = 0, live: Boolean = false, keys: Set[String] = Set()): Seq[Record] = {
    if (enabled) super.query(queryMap, limit, live, keys) else Seq.empty
  }

  override def addObserver(actor: Actor) {
    if (enabled) super.addObserver(actor)
  }
  override def delObserver(actor: Actor) {
    if (enabled) super.delObserver(actor)
  }

  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], state: StateMachine.State): Seq[Record] = {
    // generate function
    if (live) {
      if (dataStore.isDefined) {
        return dataStore.get.query(queryMap, limit, keys)
      } else {
        logger.warn("DataStore is not available, applying query to cached records")
      }
    }
    if (queryMap.isEmpty) {
      firstOf(limit, localState(state).records)
    } else {
      firstOf(limit, localState(state).records.filter(record => ctx.recordMatcher.doesMatch(queryMap, record.toMap)))
    }
  }

  protected def load(): Seq[Record] = {
    if (dataStore.isDefined) {
      dataStore.get.load()
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

  protected def delta(newRecords: Seq[Record], oldRecords: Seq[Record]): Delta = {
    val oldMap = oldRecords.map(rec => rec.id -> rec).toMap
    val newMap = newRecords.map(rec => rec.id -> rec).toMap

    val now = DateTime.now

    val removedMap = oldMap.filterNot(pair => newMap.contains(pair._1)).map(
      pair => pair._1 -> pair._2.copy(mtime = now, ltime = now))
    val addedMap = newMap.filterNot(pair => oldMap.contains(pair._1))

    val changes = newMap.filter(pair => {
      oldMap.contains(pair._1) && !pair._2.sameData(oldMap(pair._1))
    }).map(pair => pair._1 -> RecordUpdate(oldMap(pair._1).copy(mtime = now, ltime = now), pair._2))

    // need to reset stime, ctime, tags for crawled records to match what we have in memory
    val fixedRecords = newRecords.collect {
      case rec: Record if changes.contains(rec.id) =>
        oldMap(rec.id).copy(data = rec.data, mtime = rec.mtime, stime = rec.stime)
      case rec: Record if oldMap.contains(rec.id) =>
        oldMap(rec.id).copy(data = rec.data, mtime = rec.mtime)
      case rec: Record => rec
    }

    logger.info(this + " total: " + fixedRecords.size + " changed: " + changes.size + " added: " + addedMap.size + " removed: " + removedMap.size)
    Delta(fixedRecords, changed = changes.values.toSeq, added = addedMap.values.toSeq, removed = removedMap.values.toSeq)
  }

  protected override def initState = addInitialState(super.initState, newLocalState(CollectionState(records = load())))

  protected override def init() {
    Monitors.registerObject("edda.collection." + name, this)
    if (dataStore.isDefined) {
      dataStore.get.init()
    }
    Option(elector).foreach(_.addObserver(this))
    Option(crawler).foreach(_.addObserver(this))
    // listen to our own DeltaResult events
    // it is a sync call so put it in another actor
    Actor.actor {
      this.addObserver(this)
    }
    refresher()
  }

  def timeLeft(lastRun: DateTime, millis: Long): Long = {
    val timeLeft = millis - (DateTime.now.getMillis - lastRun.getMillis)
    if (timeLeft < 0) 0 else timeLeft
  }

    protected def refresher() {
        if (Option(crawler) == None || Option(elector) == None) return
        val refresh = Utils.getProperty(ctx.config, "edda.collection", "refresh", name, "60000").toLong
        val cacheRefresh = Utils.getProperty(ctx.config, "edda.collection", "cache.refresh", name, "10000").toLong
        NamedActor(this + " refresher") {
            elector.addObserver(Actor.self)
            var amLeader = elector.isLeader
            // crawl immediately the first time
            if (amLeader) crawler.crawl()

            var lastRun = DateTime.now
            Actor.loop {
                val timeout = if (amLeader) refresh else cacheRefresh
                try {
                    Actor.reactWithin(timeLeft(lastRun, timeout)) {
                        case TIMEOUT => {
                            if (amLeader) crawler.crawl() else this ! Load(this)
                            lastRun = DateTime.now
                        }
                        case Elector.ElectionResult(from, result) => {
                            // if we just became leader, then start a crawl
                            if (!amLeader && result) {
                                this !? (300000,SyncLoad(this)) match {
                                    case Some(OK(frm)) => Unit
                                    case None => throw new java.lang.RuntimeException("TIMEOUT: " + this + " Failed to reload data as we became leader in 5m")
                                }
                                crawler.crawl()
                                lastRun = DateTime.now
                            }
                            amLeader = result
                        }
                        case message => {
                            logger.error("Invalid message " + message + " from sender " + sender)
                        }
                    }
                } catch {
                    case e: Exception => logger.error(this + " failed to refresh", e)
                }
            }
        }
    }

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
        DateTime.now.getMillis - lastCrawl.getMillis
      }
    })

  private def doLoad(): Seq[Record] = {
    val stopwatch = loadTimer.start()
    val records = try {
      val now = DateTime.now
      load().map(_.copy(mtime=now))
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

  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (SyncLoad(from), state) => {
      // SyncLoad allows us to make sure we have a current cache in memory of "live" records
      // before we take over as "Leader" and start writing to the DataStore
      NamedActor(this + " SyncLoad processor") {
        val records = doLoad()
        this ! Crawler.CrawlResult(this, if (records.size == 0) localState(state).records else records)
        sender ! OK(this)
      }
      state
    }
    case (Load(from), state) => {
      NamedActor(this + " Load processor") {
        val records = doLoad()
        this ! Crawler.CrawlResult(this, if (records.size == 0) localState(state).records else records)
      }
      state
    }
    case (Crawler.CrawlResult(from, newRecords), state) => {
      // only propagate if newRecords are not the same as the last crawled result
      lastCrawl = DateTime.now
      if (newRecords ne localState(state).crawled) {
        NamedActor(this + " CrawlResult processor") {
          val d: Delta =
            if (from == this) {
              // this is from a Load so no need to calculate Delta
              Delta(newRecords, Seq(), Seq(), Seq())
            } else {
              delta(newRecords, localState(state).records)
            }

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

          Observable.localState(state).observers.foreach(_ ! DeltaResult(this, d))
        }
        setLocalState(state, localState(state).copy(crawled = newRecords))
      } else state
    }
    case (DeltaResult(from, d), state) => {
      // only propagate if the delta records are not the same as the current cached records
      if (d.records ne localState(state).records) {
        if (localState(state).amLeader) {
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
    case (Elector.ElectionResult(from, result), state) => {
      setLocalState(state, localState(state).copy(amLeader = result))
    }
  }

  override protected def transitions = localTransitions orElse super.transitions

  override def toString = "[Collection " + name + "]"

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

  override def stop() {
    logger.info("Stoping " + this)
    Option(elector).foreach(_.stop())
    Option(crawler).foreach(_.stop())
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
