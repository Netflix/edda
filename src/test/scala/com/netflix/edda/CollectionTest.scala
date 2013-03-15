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

import org.slf4j.LoggerFactory

import com.netflix.edda.basic.BasicContext

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.joda.time.DateTime

import java.util.Properties
import com.netflix.config.DynamicPropertyFactory
import com.netflix.config.ConcurrentCompositeConfiguration

import org.apache.commons.configuration.MapConfiguration


class CollectionTest extends FunSuite with BeforeAndAfter {
  import Utils._
  import Queryable._

  val logger = LoggerFactory.getLogger(getClass)

  before {
    Utils.initConfiguration("edda.properties")
  }

  test("load") {
    val coll = new TestCollection
    coll.start()

    SYNC {
      coll.query(Map("id" -> "b")) {
        case Success(results: QueryResult) => {
          expect(Nil) { results.records }
        }
      }
    }

    // dont let the crawler reset our records
    coll.elector.leader = false
    coll.dataStore.get.records = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    coll ! Collection.Load(coll)
    // allow for collection to load
    Thread.sleep(1000)
    SYNC {
      coll.query(Map("id" -> "b")) {
        case Success(results: QueryResult) => {
          expect(1) {
            results.records.size
          }
          expect(2) {
            results.records.head.data
          }
          expect("b") {
            results.records.head.id
          }
          coll.stop()
        }
      }
    }
  }
  
  test("update") {
    val coll = new TestCollection
    coll.dataStore.get.records = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    coll.start()


    SYNC {
      coll.query() {
        case Success(results: QueryResult) => {
          expect(3) { results.records.size }
        }
      }
    }

    coll.crawler.records = Seq(Record("a", 1), Record("b", 3), Record("c", 4), Record("d", 5))
    coll.crawler.crawl()
    // allow for crawl to propagate
    Thread.sleep(1000)

    SYNC {
      coll.query(Map("data" -> Map("$gte" -> 3))) {
        case Success(results: QueryResult) => {
          expect(3) { results.records.size }
        }
      }
    }
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

    coll.dataStore.get.records = dataStoreResults
    coll.start()

    SYNC {
      // expect data loaded form dataStore
      coll.query() {
        case Success(results: QueryResult) => {
          expect(3) { results.records.size }
        }
      }
    }

    // set crawler results and wait for the crawler results to propagate
    coll.crawler.records = crawlResults
    Thread.sleep(1000)

    SYNC {
      // we should get 4 records now
      coll.query() {
        case Success(results: QueryResult) => {
          expect(4) { results.records.size }
        }
      }
    }

    // now drop leader role and wait for dataStore results to reload
    // but first set the ltime on the "a" record so it will be
    // removed from the record set upon reload
    coll.elector.leader = false
    val newA = coll.dataStore.get.records.head.copy(ltime=DateTime.now())
    coll.dataStore.get.records = newA +: coll.dataStore.get.records.tail
    Thread.sleep(1000)

    SYNC {
      coll.query() {
        case Success(results: QueryResult) => {
          expect(3) { results.records.size }
        }
      }
    }

    SYNC {
      coll.query(Map("id" -> "a")) {
        case Success(results: QueryResult) => {
          expect(0) { results.records.size }
        }
      }
    }
  }
}
