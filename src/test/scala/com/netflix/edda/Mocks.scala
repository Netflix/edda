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

import com.netflix.edda.basic.BasicContext
import org.joda.time.DateTime

class TestDatastore extends Datastore {
  var recordSet = RecordSet()

  def init() {
  }

  override 
  def query(queryMap: Map[String, Any], limit: Int, keys: Set[String], replicaOk: Boolean)(implicit req: RequestId): Seq[Record] = {
    recordSet.records
  }

  override
  def load(replicaOk: Boolean)(implicit req: RequestId): RecordSet = {
    recordSet
  }

  override 
  def update(d: Collection.Delta)(implicit req: RequestId): Collection.Delta = {
    recordSet = d.recordSet
    d
  }

  override
  def remove(queryMap: Map[String, Any])(implicit req: RequestId) {
    recordSet.records
  }

}


class TestCrawler(val name: String = "TestCrawler") extends Crawler {
  var records = Seq[Record]()

  protected def doCrawl()(implicit req: RequestId): Seq[Record] = records
}


class TestElector extends Elector {
  var leader = true

  protected def runElection()(implicit req: RequestId): Boolean = leader
}

class TestCollection(val name: String = "test.collection") extends Collection(BasicContext) {
  val crawler = new TestCrawler(name + " Crawler")
  override
  lazy val dataStore = Some(new TestDatastore)
  val elector = new TestElector
}

