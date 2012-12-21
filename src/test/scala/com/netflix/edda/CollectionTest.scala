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

import org.joda.time.DateTime

class CollectionTest extends FunSuite {
  val logger = LoggerFactory.getLogger(getClass)
  test("load") {
    val coll = new TestCollection
    coll.start()
    expect(Nil) {
      coll.query(Map("id" -> "b"))
    }

    // dont let the crawler reset our records
    coll.elector.leader = false
    coll.dataStore.get.records = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    coll ! Collection.Load(coll)
    // allow for collection to load
    Thread.sleep(1000)
    val records = coll.query(Map("id" -> "b"))
    expect(1) {
      records.size
    }
    expect(2) {
      records.head.data
    }
    expect("b") {
      records.head.id
    }
    coll.stop()
  }

  test("update") {
    val coll = new TestCollection
    coll.dataStore.get.records = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    coll.start()

    expect(3) {
      coll.query().size
    }

    coll.crawler.records = Seq(Record("a", 1), Record("b", 3), Record("c", 4), Record("d", 5))
    coll.crawler.crawl()
    // allow for crawl to propagate
    Thread.sleep(1000)

    val records = coll.query(Map("data" -> Map("$gte" -> 3)))

    expect(3) {
      records.size
    }
  }

  test("leader") {
    // check for election results every 100ms
    BasicContext.config.setProperty("edda.elector.refresh", "200")
    // collection should crawl every 100ms
    BasicContext.config.setProperty("edda.collection.refresh", "200")
    BasicContext.config.setProperty("edda.collection.cache.refresh", "200")
    val coll = new TestCollection
    val dataStoreResults = Seq(Record("a", 1), Record("b", 2), Record("c", 3))
    val crawlResults = Seq(Record("a", 1), Record("b", 3), Record("c", 4), Record("d", 5))

    coll.dataStore.get.records = dataStoreResults
    coll.start()

    // expect data loaded form dataStore
    expect(3) {
      coll.query().size
    }

    // set crawler results and wait for the crawler results to propagate
    coll.crawler.records = crawlResults
    Thread.sleep(1000)

    // we should get 4 records now
    expect(4) {
      coll.query().size
    }

    // now drop leader role and wait for dataStore results to reload
    // but first set the ltime on the "a" record so it will be
    // removed from the record set upon reload
    coll.elector.leader = false
    val newA = coll.dataStore.get.records.head.copy(ltime=DateTime.now())
    coll.dataStore.get.records = newA +: coll.dataStore.get.records.tail
    Thread.sleep(1000)

    expect(3) {
      coll.query().size
    }

    // verify that "a" was removed after ltime was set 
    expect(0) {
      coll.query(Map("id" -> "a")).size
    }    
  }
}
