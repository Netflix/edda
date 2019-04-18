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

import org.joda.time.DateTime

/** basic interface for data stores to persist Crawler/Collection state */
trait Datastore {
  /** setup data store connections */
  def init()

  /** perform query on data store, see [[com.netflix.edda.Queryable.query]] */
  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean)(implicit req: RequestId): Seq[Record]

  /** load records from data store, used at Collection start-up to prime in-memory cache and to refresh
    * in-memory cache when we are not the leader
    *
    * @param replicaOk specify if we can load from a read-replica in the data store when there are
    *                  redundant systems running for high-availability.
    */
  def load(replicaOk: Boolean)(implicit req: RequestId): RecordSet

  /** make changes to the data store depending on the Collection delta found after a Crawl result */
  def update(d: Collection.Delta)(implicit req: RequestId): Collection.Delta

  /** remove records that match the query */
  def remove(queryMap: Map[String, Any])(implicit req: RequestId)
}
