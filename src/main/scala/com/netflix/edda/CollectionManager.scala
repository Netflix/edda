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

import org.slf4j.LoggerFactory

object CollectionManager {
  private[this] val logger = LoggerFactory.getLogger(getClass)
  var collections: Map[String, Queryable] = Map()

  def register(name: String, collection: Queryable) {
    if (logger.isInfoEnabled) logger.info("Registering collection " + collection)
    collections = collections + (name -> collection)
  }

  def get(name: String): Option[Queryable] = {
    collections.get(name)
  }

  def names(): Set[String] = collections.keySet

  def start()(implicit req: RequestId) {
    if (logger.isInfoEnabled) logger.info(s"$req Starting collections")
    collections.values.foreach(_.start())
  }

  def stop()(implicit req: RequestId) {
    if (logger.isInfoEnabled) logger.info(s"$req Stopping collections")
    collections.values.foreach(_.stop())
  }
}
