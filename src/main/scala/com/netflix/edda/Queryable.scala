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

import com.netflix.servo.monitor.Monitors

object Queryable extends StateMachine.LocalState[CollectionState] {
  private case class Query(from: Actor, query: Map[String, Any], limit: Int, live: Boolean, keys: Set[String]) extends StateMachine.Message
  private case class QueryResult(from: Actor, records: Seq[Record]) extends StateMachine.Message {
    override def toString = "QueryResult(records=" + records.size + ")"
  }
}

abstract class Queryable extends Observable {
  import Queryable._

  private[this] val queryTimer = Monitors.newTimer("query")
  private[this] val queryCounter = Monitors.newCounter("query.count")
  private[this] val queryErrorCounter = Monitors.newCounter("query.errors")

  def query(queryMap: Map[String, Any] = Map(), limit: Int = 0, live: Boolean = false, keys: Set[String] = Set()): Seq[Record] = {
    val self = this
    val stopwatch = queryTimer.start()
    self !? (60000, Query(self, queryMap, limit, live, keys)) match {
      case Some(QueryResult(`self`, results)) => {
          stopwatch.stop()
          queryCounter.increment()
          results
      }
      case None => {
          stopwatch.stop()
          queryErrorCounter.increment()
          throw new java.lang.RuntimeException("TIMEOUT: " + this + " Failed to fetch query results within 60s for query: " + queryMap + " limit: " + limit + " keys: " + keys)
      }
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
