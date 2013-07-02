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
import scala.concurrent.ExecutionContext.Implicits.global

import org.slf4j.LoggerFactory

/** a pseudo collection made up of other related collections.  This allows multiple collections
  * of the same type (but crawled for different accounts) to appear as one unified
  * collection
  * @param name the root name of the collection (usually name from RootCollection base class)
  * @param collections list of common collections that should appear unified
  */
class MergedCollection(val name: String, val collections: Seq[Collection]) extends Queryable {
  override def toString = "[MergedCollection " + name + "]"

  import Queryable._
  import Utils._

  private[this] val logger = LoggerFactory.getLogger(getClass)

  // // helper function to run query and make reply
  // def queryRunner(coll: Collection, query: Query, replyTo: scala.actors.OutputChannel[Any]): Unit = {
  //   coll.query(query.query, query.limit, query.live, query.keys, query.replicaOk) onComplete {
  //     case scala.util.Success(recs: Seq[Record]) => 
  //       if (logger.isDebugEnabled) logger.debug(Actor.self + " sending query records to -> " + replyTo)
  //       replyTo ! results
  //     }
  //     case scala.util.Failure(error) => {
  //       if (logger.isErrorEnabled) logger.error("query on " + coll + " failed: " + query + " with error: " + error) 
  //       val msg = QueryError(this, error)
  //       if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + replyTo)
  //       replyTo ! msg
  //     }
  //   }
  // }

  /** handle Query Message for MergedCollection */
  private def localTransitions: PartialFunction[(Any, StateMachine.State), StateMachine.State] = {
    // case (query @ Query(from, queryMap, limit, live, keys, replicaOk), state) => {
    //   val replyTo = sender

    //   Utils.NamedActor(this + " Query processor") {
    //     if( collections.size == 1 ) {
    //       queryRunner(collections.head, query, replyTo)
    //     } else {
    //       // handle multiple results to all collections
    //       val merger = Utils.NamedActor(this + " query merger") {
    //         var expected = collections.size
    //         var merged: Seq[Record] = Seq()
    //         Actor.self.loopWhile( expected > 0 ) {
    //           Actor.self.react {
    //             case QueryResult(from, results) => {
    //               merged = merged ++ results
    //               expected -= 1
    //               if( expected == 0 ) {
    //                 val msg = QueryResult(this, firstOf(limit, merged.sortWith((a, b) => a.stime.isAfter(b.stime))))
    //                 if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + replyTo)
    //                 replyTo ! msg
    //               }
    //             }
    //             case msg @ QueryError(from, error) => {
    //               replyTo ! msg
    //               expected = 0
    //             }
    //           }
    //         }
    //       }
    //       collections.foreach( queryRunner(_, query, merger) )
    //     }
    //   }
    //   state
    // }

    case (query @ Query(from, queryMap, limit, live, keys, replicaOk), state) => {
      val replyTo = sender
      scala.concurrent.future {
        if( collections.size == 1 ) {
          collections.head.query(query.query, query.limit, query.live, query.keys, query.replicaOk) onComplete {
            case scala.util.Success(recs: Seq[Record]) => {
              val msg = QueryResult(this, recs)
              if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + replyTo)
              replyTo ! msg
            }
            case scala.util.Failure(error) => {
              if (logger.isErrorEnabled) logger.error("query on " + collections.head + " failed: " + query + " with error: " + error) 
              val msg = QueryError(this, error)
              if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + replyTo)
              replyTo ! msg
            }
          }
        } else {
          val futures = collections.map( _.query(query.query, query.limit, query.live, query.keys, query.replicaOk) )
          try {
            val recs = futures.flatMap( f => scala.concurrent.Await.result(f, scala.concurrent.duration.Duration(60000, scala.concurrent.duration.MILLISECONDS) ) )
            val msg = QueryResult(this, firstOf(limit, recs.sortWith((a, b) => a.stime.isAfter(b.stime))))
            if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + replyTo)
            replyTo ! msg
          }
          catch {
            case e: Exception => {
              val msg = QueryError(this, e)
              if (logger.isDebugEnabled) logger.debug(Actor.self + " sending: " + msg + " -> " + replyTo)
              replyTo ! msg
            }
          }
        }
      }
      state
    }
  }

  override protected def transitions = localTransitions orElse super.transitions

  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean, state: StateMachine.State): Seq[Record] = { throw new java.lang.RuntimeException("doQuery on MergedCollection should not be called") }

  /** start the actors for all the merged collections then start this actor */
  override def start() = {
    if (logger.isInfoEnabled) logger.info("Starting " + this)
    collections.foreach(_.start())
    super.start()
  }

  /** stop the actors for all the merged collections then stop this actor */
  override def stop() {
    if (logger.isInfoEnabled) logger.info("Stopping " + this)
    collections.foreach(_.stop())
    super.stop()
  }
}
