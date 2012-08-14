package com.netflix.edda
import scala.actors.Actor

case class CrawlerState(records: List[Record] = List[Record]())

object Crawler extends StateMachine.LocalState[CrawlerState] {
    // Message sent to Observers
    case class CrawlResult(newRecords: List[Record]) extends StateMachine.Message

    // internal messages
    private case class Crawl() extends StateMachine.Message
}

trait CrawlerComponent {
    val crawler: Crawler
}

trait Crawler extends Observable {
    import Crawler._
    def crawl() = this ! Crawl()
    
    protected def doCrawl(): List[Record]

    protected override
    def initState = addInitialState(super.initState, newLocalState(CrawlerState()))

    private
    def localTransitions: PartialFunction[(Any,StateMachine.State),StateMachine.State] = {
        case (Crawl(),state) => {
            // this is blocking so we dont crawl in parallel
            val newRecords = doCrawl()
            Observable.localState(state).observers.foreach( _ ! Crawler.CrawlResult(newRecords) )
            setLocalState(state, CrawlerState(newRecords))
        }
    }

    protected override
    def transitions = localTransitions orElse super.transitions
}
