/*
 * Copyright 2012-2019 Netflix, Inc.
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
import scala.actors.scheduler.ExecutorScheduler

import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.Callable

import com.netflix.servo.monitor.Monitors
import com.netflix.servo.monitor.MonitorConfig
import com.netflix.servo.monitor.BasicGauge
import com.netflix.servo.DefaultMonitorRegistry

import org.joda.time.DateTime

import org.slf4j.LoggerFactory

case class CollectionProcessorState(recordSet: RecordSet = RecordSet())
object CollectionProcessor extends StateMachine.LocalState[CollectionProcessorState] {

  /** Message sent to observers after a collection has been updated */
  case class DeltaResult(from: Actor, delta: Collection.Delta, origMeta: Map[String,Any] = Map())(implicit req: RequestId) extends StateMachine.Message

  /** Message to Load the record set from the Datastore */
  case class Load(from: Actor)(implicit req: RequestId) extends StateMachine.Message

  /** Messsage to *Synchronously* Load the record set from the Datastore */
  case class SyncLoad(from: Actor)(implicit req: RequestId) extends StateMachine.Message

  /** Response from the SyncLoad request */
  case class OK(from: Actor)(implicit req: RequestId) extends StateMachine.Message

}

class CollectionProcessor(collection: Collection) extends Observable {
  import CollectionProcessor._
  val logger = LoggerFactory.getLogger(getClass)

  protected override def initState = addInitialState(super.initState, newLocalState(CollectionProcessorState(recordSet = Collection.localState(collection.initState).recordSet)))
  override def toString = s"[Collection Processor ${collection.name}]"

  override def threadPoolSize = 1
  
  lazy val logDiffs = Utils.getProperty("edda.collection", "logDiffs", collection.name, "true")

  private[this] val updateTimer = Monitors.newTimer("update")
  private[this] val updateCounter = Monitors.newCounter("update.count")
  private[this] val updateErrorCounter = Monitors.newCounter("update.errors")

  private[this] var lastCrawl = DateTime.now
  private[this] val crawlGauge = new BasicGauge[java.lang.Long](
    MonitorConfig.builder("lastCrawl").build(),
    new Callable[java.lang.Long] {
      def call() = {
        if (collection.elector.isLeader()(RequestId("lastCrawlGauge"))) {
          DateTime.now.getMillis - lastCrawl.getMillis
        } else 0
      }
    })

  private[this] var lastLoad = DateTime.now
  private[this] val loadGauge = new BasicGauge[java.lang.Long](
    MonitorConfig.builder("lastLoad").build(),
    new Callable[java.lang.Long] {
      def call() = {
        if (collection.elector.isLeader()(RequestId("lastLoadGauge"))) {
          0
        } else DateTime.now.getMillis - lastLoad.getMillis
      }
    })

  // eliminate used-only-once warnings from IntelliJ
  if(false) crawlGauge
  if(false) loadGauge

  override protected def transitions = localTransitions orElse super.transitions
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ SyncLoad(from), state) => {
      implicit val req = gotMsg.req
      // SyncLoad allows us to make sure we have a current cache in memory of "live" records
      // before we take over as "Leader" and start writing to the Datastore
      flushMessages {
        case SyncLoad(from) => true
      }
      val replyTo = sender
      val recordSet = collection.doLoad(replicaOk = false)
      val msg = Crawler.CrawlResult(this, recordSet)
      if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $this")
      this ! msg
      val msg2 = OK(this)
      if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg2 -> $replyTo")
      replyTo ! msg2
      lastLoad = DateTime.now
      state
    }
    case (gotMsg @ Load(from), state) => {
      implicit val req = gotMsg.req
      flushMessages {
        case Load(from) => true
      }
      val recordSet = try {
        if (logger.isInfoEnabled) logger.info(s"$req$this doing full reload of collection");
        collection.doLoad(replicaOk = true)
      } catch {
        case e: Exception => {
          logger.error(s"$req$this failed to load", e)
          throw e
        }
      }
      val msg = Crawler.CrawlResult(this, recordSet)
      if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $this")
      this ! msg
      lastLoad = DateTime.now
      state
    }
    case (gotMsg @ Crawler.CrawlResult(from, newRecordSet), state) => {
      implicit val req = gotMsg.req
      lastCrawl = DateTime.now
      def processDelta(d: Collection.Delta) = {
        lazy val path = collection.name.replace('.', '/')
        d.added.foreach(
          rec => {
            if (logger.isInfoEnabled) logger.info("{} Added {}/{};_pp;_at={}", Utils.toObjects(req, path, rec.id, rec.stime.getMillis))
          })
        d.removed.foreach(
          rec => {
            if (logger.isInfoEnabled) logger.info("{} Removing {}/{};_pp;_at={}", Utils.toObjects(req, path, rec.id, rec.stime.getMillis))
          })
        d.changed.foreach(
          update => {
            if( logDiffs.get.toBoolean ) {
              lazy val diff: String = Utils.diffRecords(Array(update.newRecord, update.oldRecord), Some(1), path)
              if (logger.isInfoEnabled) logger.info(s"$req\n$diff")
            } else {
              if( logger.isInfoEnabled) {
                logger.info(s"$req Updated $path/${update.newRecord.id};_pp;_at=${update.newRecord.stime.getMillis} previous=${update.oldRecord.stime.getMillis}")
              }
            }
          })
        
        val msg = DeltaResult(this, d, localState(state).recordSet.meta)
        Observable.localState(state).observers.foreach(o => {
          if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $o")
          o ! msg
        })
      }
      
      if (from == this || !collection.elector.isLeader()) {
        // this is from a Load so no need to calculate Delta
        // just make sure there are not dups loaded
        val seen = scala.collection.mutable.Set[String]()
        val uniqRecs = newRecordSet.records.filter(r => {
          val in = seen.contains(r.id)
          if( !in ) seen += r.id
          !in
        })
        processDelta(Collection.Delta(RecordSet(uniqRecs,newRecordSet.meta), Seq(), Seq(), Seq()))
      } else {
        processDelta(collection.delta(newRecordSet, localState(state).recordSet))
      }
      state
    }
    case (gotMsg @ DeltaResult(from, d, origMeta), state) => {
      implicit val req = gotMsg.req
      if( origMeta("req").asInstanceOf[String] != localState(state).recordSet.meta("req").asInstanceOf[String] ) {
        val origReq = origMeta("req").asInstanceOf[String]
        logger.error(s"$req$this ignoring delta results, compared against old state: $origReq")
        state
      } else if( d.recordSet.meta("source") == "crawl" && !collection.elector.isLeader() ) {
        logger.error(s"$req$this ignoring delta result from crawl, no longer leader")
        state
      } else {
        if (collection.elector.isLeader()) {
          val stopwatch = updateTimer.start()
          val newState = try {
            val newDelta = d.recordSet.meta.get("source") match {
              // only call update is the source is a crawler, if it was just
              // loaded then we dont need to call update
              case Some("crawl") => {
                val newDelta = collection.update(d)
                updateCounter.increment()
                newDelta
              }
              case _ => d
            }
            val msg = Collection.UpdateOK(this, newDelta, origMeta)
            if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $collection")
            collection ! msg
            setLocalState(state, localState(state).copy(recordSet = newDelta.recordSet))
          } catch {
            case e: Exception => {
              updateErrorCounter.increment()
              logger.error(s"$req$this failed to update", e)
              throw e
            }
          } finally {
            stopwatch.stop()
          }
          if (logger.isInfoEnabled) logger.info("{}{} Updated {} {} records(Changed: {}, Added: {}, Removed: {}) in {} sec", Utils.toObjects(
            req, this, d.recordSet.records.size, d.recordSet.meta, d.changed.size, d.added.size, d.removed.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000.0 -> "%.2f"))
          newState
        } else {
          val msg = Collection.UpdateOK(this, d, origMeta)
          if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $collection")
          collection ! msg
          setLocalState(state, localState(state).copy(recordSet = d.recordSet))
        }
      }
    }
  }

  override def start() = {
    Monitors.registerObject("edda.collection.processor." + collection.name, this)
    DefaultMonitorRegistry.getInstance().register(Monitors.newThreadPoolMonitor(s"edda.collection.processor.${collection.name}.threadpool", this.pool.asInstanceOf[ThreadPoolExecutor]))
    super.start()
  }
}
