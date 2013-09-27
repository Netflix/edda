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

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.joda.time.DateTime

import java.util.Properties
import com.netflix.config.DynamicPropertyFactory
import com.netflix.config.ConcurrentCompositeConfiguration

import org.apache.log4j.Logger
import org.apache.log4j.Level

import org.apache.commons.configuration.MapConfiguration

import scala.concurrent._
import scala.concurrent.duration._
import scala.concurrent.ExecutionContext.Implicits.global

class CollectionTest extends FunSuite with BeforeAndAfter {
  import Utils._
  import Queryable._

  implicit val req = RequestId()

  def SYNC[T](future: Awaitable[T]): T = {
    Await.result(future, Duration(5, SECONDS))
  }

  val logger = Logger.getRootLogger()

  before {
    Utils.initConfiguration("edda.properties")
  }

  test("load") {
    val coll = new TestCollection
    coll.start()

    expectResult(Nil) {
      SYNC ( coll.query(Map("id" -> "b")) ) 
    }
    
    // dont let the crawler reset our records
    coll.elector.leader = false
    coll.dataStore.get.recordSet = coll.dataStore.get.recordSet.copy(records = Seq(Record("a", 1), Record("b", 2), Record("c", 3)))
    coll.processor ! CollectionProcessor.Load(coll)
    // allow for collection to load
    
    Thread.sleep(1000)
    val records = SYNC( coll.query(Map("id" -> "b")) )
    expectResult(1) {
      records.size
    }
    expectResult(2) {
        records.head.data
    }
    expectResult("b") {
      records.head.id
    }
    coll.stop()
  }

  test("update") {
    val coll = new TestCollection
    coll.dataStore.get.recordSet = coll.dataStore.get.recordSet.copy(records = Seq(Record("a", 1), Record("b", 2), Record("c", 3)))
    coll.start()


    expectResult(3) {
      SYNC( coll.query() ).size
    }

    coll.crawler.records = Seq(Record("a", 1), Record("b", 3), Record("c", 4), Record("d", 5))
    coll.crawler.crawl()
    // allow for crawl to propagate
    Thread.sleep(1000)

    expectResult(3) {
      SYNC( coll.query(Map("data" -> Map("$gte" -> 3))) ).size 
    }
    coll.stop()
  }

  test("leader") {
    DynamicPropertyFactory.getInstance()
    val composite = DynamicPropertyFactory. getBackingConfigurationSource.asInstanceOf[ConcurrentCompositeConfiguration]
    val config = new MapConfiguration(new Properties);
    composite.addConfigurationAtFront(config, "testConfig")

    // check for election results every 100ms
    config.addProperty("edda.elector.refresh", "200")
    // collection should crawl every 100ms
    config.addProperty("edda.collection.refresh", "200")
    config.addProperty("edda.collection.cache.refresh", "200")
    val coll = new TestCollection
    val dataStoreResults = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    val crawlResults = Seq(Record("a", 1), Record("b", 3), Record("c", 4), Record("d", 5))

    coll.dataStore.get.recordSet = coll.dataStore.get.recordSet.copy(records = dataStoreResults)
    coll.start()

    // expect data loaded form dataStore
    expectResult(3) {
      SYNC( coll.query() ).size
    }
    
    // set crawler results and wait for the crawler results to propagate
    coll.crawler.records = crawlResults
    coll.crawler.crawl()
    Thread.sleep(1000)
    
    // we should get 4 records now
    expectResult(4) {
      SYNC( coll.query() ).size
    }

    // now drop leader role and wait for dataStore results to reload
    // but remove the "a" record
    coll.elector.leader = false
    // wait for previous crawls to propogate
    Thread.sleep(300)
    coll.dataStore.get.recordSet = coll.dataStore.get.recordSet.copy(records = coll.dataStore.get.recordSet.records.tail)
    coll.processor ! CollectionProcessor.Load(coll)
    Thread.sleep(300)

    expectResult(3) {
      SYNC( coll.query() ).size
    }

    expectResult(0) {
      SYNC( coll.query(Map("id" -> "a")) ).size
    }

    coll.stop()
  }
}
