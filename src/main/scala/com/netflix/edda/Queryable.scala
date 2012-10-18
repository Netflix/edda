package com.netflix.edda

import scala.actors.Actor

object Queryable extends StateMachine.LocalState[CollectionState] {
  private case class Query(from: Actor, query: Map[String, Any], limit: Int, live: Boolean, keys: Set[String]) extends StateMachine.Message
  private case class QueryResult(from: Actor, records: Seq[Record]) extends StateMachine.Message {
    override def toString = "QueryResult(records=" + records.size + ")";
  }
}

abstract class Queryable extends Observable {
  import Queryable._

  def query(queryMap: Map[String, Any] = Map(), limit: Int = 0, live: Boolean = false, keys: Set[String] = Set()): Seq[Record] = {
    val self = this
    self !? (60000, Query(self, queryMap, limit, live, keys)) match {
      case Some(QueryResult(`self`, results)) => results
      case None => throw new java.lang.RuntimeException("TIMEOUT: Failed to fetch query results within 60s")
    }
  }

  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], state: StateMachine.State): Seq[Record]

  protected def firstOf(limit: Int, records: Seq[Record]): Seq[Record] = {
    if (limit > 0) records.take(limit) else records
  }
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Query(from, queryMap, limit, live, keys), state) => {
      val replyTo = sender
      Utils.NamedActor(this + " Query processor") {
          replyTo ! QueryResult(this, doQuery(queryMap, limit, live, keys, state))
      }
      state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}
