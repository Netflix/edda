package com.netflix.edda
import scala.actors.Actor

import org.joda.time.DateTime

case class CrawlerState(records: List[Record] = List[Record](), crawlTime: Option[DateTime] = None)

object Crawler extends StateMachine.LocalState[CrawlerState] {
    // Message sent to Observers
    case class CrawlResult(newRecords: List[Record]) extends StateMachine.Message

    // internal messages
    private case class Crawl() extends StateMachine.Message
}

trait CrawlerComponent {
    val crawler: Crawler
}

trait Crawler extends Observable with NamedComponent with ConfigurationComponent {
    import Crawler._
    def crawl() = this ! Crawl()
    
    protected def doCrawl(): List[Record]

    protected override
    def initState = addInitialState(super.initState, newLocalState(CrawlerState()))

    val minCycle = config.getProperty(
        "edda.crawler." + name + ".minimumCycle",
        config.getProperty("edda.crawler.minimumCycle", "60000")
    ).toInt
    
    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawl(),state) => {
            val crawlTime = localState(state).crawlTime
            if( crawlTime == None || crawlTime.get.isBefore(DateTime.now().minusMillis(minCycle)) ) {
                // this is blocking so we dont crawl in parallel
                val newRecords = doCrawl()
                Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
                setLocalState(state, CrawlerState(newRecords,Some(DateTime.now)))
            } else state
        }
    }

    protected override
    def transitions = localTransitions orElse super.transitions
}
