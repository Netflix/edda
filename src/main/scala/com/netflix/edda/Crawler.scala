package com.netflix.edda
import scala.actors.Actor

import org.joda.time.DateTime

import java.util.concurrent.TimeUnit

import com.netflix.servo.monitor.Monitors

import org.slf4j.{Logger, LoggerFactory}


case class CrawlerState(records: Seq[Record] = Seq[Record](), crawlTime: Option[DateTime] = None)

object Crawler extends StateMachine.LocalState[CrawlerState] {
    // Message sent to Observers
    case class CrawlResult(from: Actor, newRecords: Seq[Record]) extends StateMachine.Message {
        override def toString = "CrawlResult(newRecords=" + newRecords.size + ")"
    }

    // internal messages
    private case class Crawl(from: Actor) extends StateMachine.Message
}

abstract class Crawler( ctx: ConfigContext ) extends Observable {
    import Crawler._
    import Utils._
    private[this] val logger = LoggerFactory.getLogger(getClass)
    lazy val enabled = Utils.getProperty(ctx.config, "edda.crawler", "enabled", name, "true").toBoolean

    def crawl() {
        if( enabled ) this ! Crawl(this)
    }
    
    def name: String
    
    private[this] val crawlTimer   = Monitors.newTimer("edda.crawler." + name + ".crawl")
    private[this] val crawlCounter = Monitors.newCounter("edda.crawler." + name + ".crawl.count")
    private[this] val errorCounter = Monitors.newCounter("edda.crawler." + name + ".crawl.errors")

    protected def doCrawl(): Seq[Record]

    protected override
    def initState = addInitialState(super.initState, newLocalState(CrawlerState()))

    protected override
    def init() {
        Monitors.registerObject("edda.crawler." + name, this);
    }

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawl(from),state) => {
            // this is blocking so we dont crawl in parallel
            val stopwatch = crawlTimer.start()
            val newRecords = try {
                doCrawl()
            } catch {
                case e => {
                    errorCounter.increment
                    throw e
                }
            } finally {
                stopwatch.stop
            }
            
            logger.info("{} Crawled {} records in {} sec", toObjects(
                this, newRecords.size, stopwatch.getDuration(TimeUnit.MILLISECONDS)/1000D -> "%.2f"
            ))
            crawlCounter.increment(newRecords.size)
            Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(this, newRecords) )
            setLocalState(state, CrawlerState(records=newRecords))
            
            // } else state
        }
    }

    protected override
    def transitions = localTransitions orElse super.transitions

    override
    def toString = "[Crawler " + name + "]"
}
