package com.netflix.edda
import scala.actors.Actor

case class CrawlerState(val records: List[Record], override val observers: List[Actor]) extends ObservableState(observers) {
    override
    def copy(observers: List[Actor]) = new CrawlerState(records,observers)
    def copy(records: List[Record] = records, observers: List[Actor] = observers) = new CrawlerState(records,observers)
}

object Crawler {
    // Message sent to Observers
    case class CrawlResult(newRecords: List[Record])
}


abstract class Crawler() extends Observable[CrawlerState] {
    def crawl() = this ! Crawl()
    
    protected def doCrawl(): List[Record]

    override
    def init() = CrawlerState(List[Record](), List[Actor]())

    // internal messages
    private case class Crawl()

    protected
    def localTransitions: PartialFunction[(Any,CrawlerState),CrawlerState] = {
        case (Crawl(),state) => {
            // this is blocking so we dont crawl in parallel
            val newRecords = doCrawl()
            state.observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
            state.copy(records = newRecords)
        }
    }

    override protected
    def transitions = localTransitions orElse super.transitions
}

