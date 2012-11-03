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

import com.netflix.servo.monitor.Monitors

/** Queryable companion object that declares StateMachine messages used to query Collections */
object Queryable {

  /** Message to to query the StateMachine */
  private case class Query(from: Actor, query: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean) extends StateMachine.Message

  /** response Message from a Query Message */
  private case class QueryResult(from: Actor, records: Seq[Record]) extends StateMachine.Message {
    override def toString = "QueryResult(records=" + records.size + ")"
  }

}

/** this class add a query routine and messages to the StateMachine that supports the query routine.
  * It has been abstracted from the Collection class so that it can be shared with MergedCollection
  */
abstract class Queryable extends Observable {

  import Queryable._

  private[this] val queryTimer = Monitors.newTimer("query")
  private[this] val queryCounter = Monitors.newCounter("query.count")
  private[this] val queryErrorCounter = Monitors.newCounter("query.errors")

  /** query a collection for Records.
   *
   * @param queryMap query criteria to select records.  See [[com.netflix.edda.basic.BasicRecordMatcher]]
   * @param limit maximum number of records to return
   * @param live boolean flag to specify if the query should go straight to the DataStore or if the in-memory cache is ok.  live=true means use the DataStore.
   * @param keys set of keynames to restrict the data fetched from the datastore.  Useful when operation on very large Records but small segment of document desired.
   * @param replicaOk boolean flag to specify if is ok for the query to be sent to a data replica in the case of a primary/secondary datastore set.
   * @return the records that match the query criteria
   */
  def query(queryMap: Map[String, Any] = Map(), limit: Int = 0, live: Boolean = false, keys: Set[String] = Set(), replicaOk: Boolean = false): Seq[Record] = {
    val self = this
    val stopwatch = queryTimer.start()
    self !?(60000, Query(self, queryMap, limit, live, keys, replicaOk)) match {
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

  /** abstract routine to perform the raw query operation for whatever DataStore used. */
  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean, state: StateMachine.State): Seq[Record]

  /** helper routine to truncate records to the specified limit if more than request records are available */
  protected def firstOf(limit: Int, records: Seq[Record]): Seq[Record] = {
    if (limit > 0) records.take(limit) else records
  }

  /** handle Query Message for StateMachine */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (Query(from, queryMap, limit, live, keys, replicaOk), state) => {
      val replyTo = sender
      Utils.NamedActor(this + " Query processor") {
        replyTo ! QueryResult(this, doQuery(queryMap, limit, live, keys, replicaOk, state))
      }
      state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}
