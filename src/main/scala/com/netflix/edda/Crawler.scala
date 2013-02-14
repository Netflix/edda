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

import java.util.concurrent.TimeUnit

import com.netflix.servo.monitor.Monitors

import org.slf4j.LoggerFactory

/** local state for Crawlers
  *
  * @param records the records that were crawled
  */
case class CrawlerState(records: Seq[Record] = Seq[Record]())

/** companion object for [[com.netflix.edda.Crawler]].  */
object Crawler extends StateMachine.LocalState[CrawlerState] {

  /** Message sent to Observers */
  case class CrawlResult(from: Actor, newRecords: Seq[Record]) extends StateMachine.Message {
    override def toString = "CrawlResult(newRecords=" + newRecords.size + ")"
  }

  /** Message to start a crawl action */
  case class Crawl(from: Actor) extends StateMachine.Message

}

/** Crawler to crawl something and generate Records based on what was crawled.
  * Those records are then passed to a Collection (typically) by sending the
  * crawl results to all observers.
 *
 * @param ctx configuration context
 */
abstract class Crawler(ctx: ConfigContext) extends Observable {

  import Crawler._
  import Utils._

  private[this] val logger = LoggerFactory.getLogger(getClass)
  lazy val enabled = Utils.getProperty(ctx.config, "edda.crawler", "enabled", name, "true").toBoolean

  /** start a crawl if the crawler is enabled */
  def crawl() {
    if (enabled) {
        val msg = Crawl(this)
        logger.debug(this + " sending: " + msg + " -> " + this)
        this ! msg
    }
  }

  /** see [[com.netflix.edda.Observable.addObserver()]].  Overridden to be a NoOp when Crawler is not enabled */
  override def addObserver(actor: Actor) {
    if (enabled) super.addObserver(actor)
  }

  /** see [[com.netflix.edda.Observable.delObserver()]].  Overridden to be a NoOp when Crawler is not enabled */
  override def delObserver(actor: Actor) {
    if (enabled) super.delObserver(actor)
  }

  /** name of the Crawler, typically matches the name of the Collection that the Crawler works with */
  def name: String

  // basic servo metrics
  private[this] val crawlTimer = Monitors.newTimer("crawl")
  private[this] val crawlCounter = Monitors.newCounter("crawl.count")
  private[this] val errorCounter = Monitors.newCounter("crawl.errors")

  /** abstract routine for subclasses to implement the actual crawl logic */
  protected def doCrawl(): Seq[Record]

  /** initilize the initial Crawler state */
  protected override def initState = addInitialState(super.initState, newLocalState(CrawlerState()))

  /** init just registers metrics for Servo */
  protected override def init() {
    Monitors.registerObject("edda.crawler." + name, this)
  }

  /** handle Crawl Messages to the StateMachine */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Crawl(from), state) => {
      // this is blocking so we don't crawl in parallel
      val stopwatch = crawlTimer.start()
      val newRecords = try {
        doCrawl()
      } catch {
        case e: Exception => {
          errorCounter.increment()
          throw e
        }
      } finally {
        stopwatch.stop()
      }

      logger.info("{} Crawled {} records in {} sec", toObjects(
        this, newRecords.size, stopwatch.getDuration(TimeUnit.MILLISECONDS) / 1000D -> "%.2f"))
      crawlCounter.increment(newRecords.size)
      Observable.localState(state).observers.foreach(o => {
          val msg = Crawler.CrawlResult(this, newRecords)
          logger.debug(this + " sending: " + msg + " -> " + o)
          o ! msg
      })
      setLocalState(state, CrawlerState(records = newRecords))

      // } else state
    }
  }

  protected override def transitions = localTransitions orElse super.transitions

  override def toString = "[Crawler " + name + "]"
}
