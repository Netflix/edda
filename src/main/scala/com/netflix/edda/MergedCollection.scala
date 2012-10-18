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

import scala.actors.Futures.{ future, awaitAll }

import org.slf4j.{ Logger, LoggerFactory }

class MergedCollection(val name: String, val collections: Seq[Collection]) extends Queryable {
  override def toString = "[MergedCollection " + name + "]"

  private[this] val logger = LoggerFactory.getLogger(getClass)

  protected def doQuery(queryMap: Map[String, Any], limit: Int, live: Boolean, keys: Set[String], state: StateMachine.State): Seq[Record] = {
    // if they have specified a subset of keys, then we need to make
    // sure stime is in there so we can sort
    val requiredKeys = if (keys.isEmpty) keys else (keys + "stime")
    val tasks = collections.map(c => future {
      c.query(queryMap, limit, live, requiredKeys)
    })
    val records = awaitAll(300000L, tasks: _*) match {
      case Nil => Seq()
      case x: Seq[Option[Seq[Record]]] => {
        //logger.info("got: " + x)
        x.collect { case Some(d) => d }.flatten
      }
      case y => {
        //logger.info("got: " + y)
        Seq()
      }
    }
    records.sortWith((a, b) => a.stime.isAfter(b.stime))
  }

  override def start() = {
    logger.info("Starting " + this);
    collections.foreach(_.start)
    super.start()
  }

  override def stop() {
    logger.info("Stoping " + this);
    collections.foreach(_.stop)
    super.stop()
  }
}
