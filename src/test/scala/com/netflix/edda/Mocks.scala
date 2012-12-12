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

import com.netflix.edda.basic.BasicContext

class TestDataStore extends DataStore {
  var records = Seq[Record]()

  def init() {
  }

  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean): Seq[Record] = {
    records
  }

  def load(replicaOk: Boolean): Seq[Record] = {
    records
  }

  def update(d: Collection.Delta) {
    records = d.records
  }
}


class TestCrawler(ctx: ConfigContext) extends Crawler(ctx) {
  val name = "TestCrawler"
  var records = Seq[Record]()

  protected def doCrawl(): Seq[Record] = records
}


class TestElector(ctx: ConfigContext) extends Elector(ctx) {
  var leader = true

  protected def runElection(): Boolean = leader
}

class TestCollection extends Collection(BasicContext) {
  val name = "test.collection"
  val crawler = new TestCrawler(BasicContext)
  val dataStore = Some(new TestDataStore)
  val elector = new TestElector(BasicContext)
}

