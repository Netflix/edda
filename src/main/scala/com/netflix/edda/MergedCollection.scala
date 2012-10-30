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

import java.util.concurrent.Executors
import java.util.concurrent.Future
import java.util.concurrent.Callable

// import scala.actors.Futures.{ future, awaitAll }

import org.slf4j.LoggerFactory

class MergedCollection(val name: String, val collections: Seq[Collection]) extends Queryable {
  override def toString = "[MergedCollection " + name + "]"

  private[this] val logger = LoggerFactory.getLogger(getClass)
  private[this] val threadPool = collections.size match {
      case 1 => None
      case _ => Some(Executors.newFixedThreadPool(collections.size * 10))
  }
    
  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], replicaOk: Boolean, state: StateMachine.State): Seq[Record] = {
    // if they have specified a subset of keys, then we need to make
    // sure stime is in there so we can sort
    val requiredKeys = if (keys.isEmpty) keys else (keys + "stime")

    if( threadPool == None ) {
        // only one collection so don't bother with futures
        collections.head.query(queryMap, limit, live, requiredKeys, replicaOk)
    } else {
        var futures: Seq[java.util.concurrent.Future[Seq[Record]]] = collections.map(
            coll => {
                threadPool.get.submit(
                    new Callable[Seq[Record]] {
                        def call() = {
                            coll.query(queryMap, limit, live, requiredKeys, replicaOk)
                        }
                    }
                )
            }
        )
        var failed: Boolean = false
        val records = futures.map(
            f => {
                try f.get 
                catch {
                    case e: Exception => {
                        failed = true
                        logger.error(this + "exception querying",e);
                        Seq()
                    }
                }
            }
        ).flatten
        
        if( failed ) {
            throw new java.lang.RuntimeException("query failed")
        }
        records.sortWith((a, b) => a.stime.isAfter(b.stime))
    }
  }

  override def start() = {
    logger.info("Starting " + this)
    collections.foreach(_.start())
    super.start()
  }

  override def stop() {
    logger.info("Stoping " + this)
    collections.foreach(_.stop())
    super.stop()
  }
}
