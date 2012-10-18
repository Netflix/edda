package com.netflix.edda

import com.netflix.edda.basic.BasicContext

class TestDatastore extends Datastore {
    var records = Seq[Record]()

    def init() {
    }
    
    def query(queryMap: Map[String, Any], limit: Int, keys: Set[String]): Seq[Record] = {
        records
    }

    def load(): Seq[Record] = {
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
    val datastore = Some(new TestDatastore)
    val elector = new TestElector(BasicContext)
}

