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
import scala.actors.TIMEOUT

import com.netflix.servo.monitor.Monitors
import org.slf4j.LoggerFactory

/** Queryable companion object that declares StateMachine messages used to query Collections */
object Queryable {

  /** Message to to query the StateMachine */
  case class Query(from: Actor, query: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean)(implicit req: RequestId) extends StateMachine.Message

  /** response Message from a Query Message */
  case class QueryResult(from: Actor, records: Seq[Record])(implicit req: RequestId) extends StateMachine.Message {
    override def toString = s"QueryResult(req=$req, records=${records.size})"
  }

  /** response Message from a Query Message */
  case class QueryError(from: Actor, error: Any)(implicit req: RequestId) extends StateMachine.Message
}

/** this class add a query routine and messages to the StateMachine that supports the query routine.
  * It has been abstracted from the Collection class so that it can be shared with MergedCollection
  */
abstract class Queryable extends Observable {

  import Queryable._
  import Utils._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  private[this] val queryTimer = Monitors.newTimer("query")
  private[this] val queryCounter = Monitors.newCounter("query.count")
  private[this] val queryErrorCounter = Monitors.newCounter("query.errors")

  def queryTimeout = 60000L

  /** query a collection for Records.
   *
   * @param queryMap query criteria to select records.  See [[com.netflix.edda.basic.BasicRecordMatcher]]
   * @param limit maximum number of records to return
   * @param live boolean flag to specify if the query should go straight to the Datastore or if the in-memory cache is ok.  live=true means use the Datastore.
   * @param keys set of keynames to restrict the data fetched from the datastore.  Useful when operation on very large Records but small segment of document desired.
   * @param replicaOk boolean flag to specify if is ok for the query to be sent to a data replica in the case of a primary/secondary datastore set.
   * @return the records that match the query criteria
   */
  def query(queryMap: Map[String, Any] = Map(), limit: Int = 0, live: Boolean = false, keys: Set[String] = Set(), replicaOk: Boolean = false)(implicit req: RequestId): scala.concurrent.Future[Seq[Record]] = {
    import QueryExecutionContext._
    val p = scala.concurrent.promise[Seq[Record]]
    Utils.namedActor(this + " query client") {
      val msg = Query(Actor.self, queryMap, limit, live, keys, replicaOk)
      if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} sending: $msg -> $this with ${queryTimeout}ms timeout")
      val stopwatch = queryTimer.start()
      this ! msg
      Actor.self.reactWithin(queryTimeout) {
        case msg @ QueryResult(from, results) => {
          stopwatch.stop()
          queryCounter.increment()
          if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} received: $msg from $sender")
          p success results
        }
        case msg @ TIMEOUT => {
          stopwatch.stop()
          queryErrorCounter.increment()
          if (logger.isDebugEnabled) logger.debug(s"$req${Actor.self} received: $msg")
          p failure new java.util.concurrent.TimeoutException("query failed  after 60000ms timeout")
        }
      }
    }
    p.future
  }

  /** abstract routine to perform the raw query operation for whatever Datastore used. */
  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean, state: StateMachine.State)(implicit req: RequestId): Seq[Record]

  /** helper routine to truncate records to the specified limit if more than request records are available */
  protected def firstOf(limit: Int, records: Seq[Record]): Seq[Record] = {
    if (limit > 0) records.take(limit) else records
  }

  /** handle Query Message for StateMachine */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    case (gotMsg @ Query(from, queryMap, limit, live, keys, replicaOk), state) => {
      implicit val req = gotMsg.req
      val replyTo = sender
      import QueryExecutionContext._
      scala.concurrent.future {
        val msg = QueryResult(this, doQuery(queryMap, limit, live, keys, replicaOk, state))
        if (logger.isDebugEnabled) logger.debug(s"$req$this sending: $msg -> $replyTo")
        replyTo ! msg
      } onFailure {
        case err: Throwable => logger.error(s"$req$this query processor failed", err)
        case err => logger.error(s"$req$this query processor failed: $err")
      }
      state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions
}
